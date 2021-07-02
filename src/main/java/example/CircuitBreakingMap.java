package example;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.*;

//TODO: Add ctor with initial capacity
public class CircuitBreakingMap<K, V> implements Map<K, V>, Releasable {
    private final CircuitBreaker circuitBreaker;
    private final Map<K, V> map;
    private long requestBytesAdded = 0;
    protected long reservedSize = 0;
    protected long reservedThreshold = 0;
    private long perElementSize = -1;
    private long perElementObjectSize = -1;
    protected int capacity;
    protected int threshold;
    protected float loadFactor = DEFAULT_LOAD_FACTOR;
    /**
     * Copied from HashMap Coretto-1.8.0_292
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingMap(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        map = newInternalMap();
    }

    public CircuitBreakingMap(CircuitBreaker circuitBreaker, int initialCapacity, float loadFactor) {
        this(circuitBreaker);
        // Copied from HashMap Coretto-1.8.0_292
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal initial capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal load factor: " +
                    loadFactor);
        this.loadFactor = loadFactor;
        this.threshold = tableSizeFor(initialCapacity);
    }

    public CircuitBreakingMap(CircuitBreaker circuitBreaker, int initialCapacity) {
        this(circuitBreaker, initialCapacity, DEFAULT_LOAD_FACTOR);
    }
    /**
     * Returns a power of two size for the given target capacity.
     */
    static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    protected Map<K, V> newInternalMap() {
        Map<K, V> map = new HashMap<>();
        addToBreaker(RamUsageEstimator.sizeOfObject(map), true);
        return map;
    }

    protected void resize() {
        // Copy pasted from HashMap
        int newCapacity, newThreshold = 0;
        if (capacity > 0) {
            if (capacity >= MAXIMUM_CAPACITY) {
                threshold = Integer.MAX_VALUE;
                return;
            } else if ((newCapacity = capacity << 1) < MAXIMUM_CAPACITY && capacity >= DEFAULT_INITIAL_CAPACITY) {
                newThreshold = threshold << 1; // double threshold
            }
        } else if (threshold > 0) {// initial capacity was placed in threshold
            newCapacity = threshold;
        } else {               // zero initial threshold signifies using defaults
            newCapacity = DEFAULT_INITIAL_CAPACITY;
            newThreshold = (int) (DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
        }

        if (newThreshold == 0) {
            float ft = (float) newCapacity * loadFactor;
            newThreshold = (newCapacity < MAXIMUM_CAPACITY && ft < (float) MAXIMUM_CAPACITY ?
                    (int) ft : Integer.MAX_VALUE);
        }
        this.threshold = newThreshold;
        this.capacity = newCapacity;
    }

    protected void addToBreaker(long bytes, boolean checkBreaker) {
        if (bytes >= 0 && checkBreaker) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, "<CircuitBreakingCollection>");
        } else {
            circuitBreaker.addWithoutBreaking(bytes);
        }
        this.requestBytesAdded += bytes;
    }

    protected void updateBreaker() {
        if (size() > threshold) {
            resize();
        }
        updateBreaker(capacity, threshold);
    }

    protected void updateBreaker(long newReservedSize, long newReservedThreshold) {
        if (perElementSize == -1) {
            Optional<Entry<K, V>> optionalEntry = map.entrySet().stream().findAny();
            assert this.size() > 0 && optionalEntry.isPresent(): "Size should have changed from 0";
            Entry<K, V> entry = optionalEntry.get();
            // there will never be elements for capacity - threshold elements
            perElementSize = RamUsageEstimator.shallowSizeOf(entry);
            perElementObjectSize = RamUsageEstimator.sizeOfObject(entry.getKey(), 0)
                    + RamUsageEstimator.sizeOfObject(entry.getValue(), 0);
        }
        long bytesDiff = (newReservedSize - reservedSize) * perElementSize
                + (newReservedThreshold - reservedThreshold) * perElementObjectSize;
        // Since this method is called after collection already grew, update reservedSize even if breaking.
        reservedSize = newReservedSize;
        reservedThreshold = newReservedThreshold;
        if (bytesDiff == 0) {
            return;
        }
        // If it breaks, then the already created data will not be accounted for.
        // So we first add without breaking, and then check.
        addToBreaker(bytesDiff, false);
        addToBreaker(0, true);
    }
    @Override
    public void close() {
        map.clear();
        updateBreaker(0, 0);
    }
    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return map.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return map.containsValue(o);
    }

    @Override
    public V get(Object o) {
        return map.get(o);
    }

    @Override
    public V put(K k, V v) {
        try {
            return map.put(k, v);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public V remove(Object o) {
        try {
            return map.remove(o);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        try {
            this.map.putAll(map);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void clear() {
        try {
            map.clear();
        } finally {
            updateBreaker();
        }
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakingMap<?, ?> that = (CircuitBreakingMap<?, ?>) o;
        return requestBytesAdded == that.requestBytesAdded &&
                reservedSize == that.reservedSize &&
                reservedThreshold == that.reservedThreshold &&
                capacity == that.capacity &&
                threshold == that.threshold &&
                loadFactor == that.loadFactor &&
                perElementSize == that.perElementSize &&
                perElementObjectSize == that.perElementObjectSize &&
                Objects.equals(circuitBreaker, that.circuitBreaker) &&
                Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(circuitBreaker, map, requestBytesAdded, reservedSize, reservedThreshold, capacity, threshold,
                loadFactor, perElementSize, perElementObjectSize);
    }
}
