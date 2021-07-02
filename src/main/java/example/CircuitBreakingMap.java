package example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.*;

public class CircuitBreakingMap<K, V> implements Map<K, V>, Releasable {
    private final CircuitBreaker circuitBreaker;
    private final Map<K, V> map;
    private long requestBytesAdded = 0;
    protected long reservedSize = 0;
    private long perElementSize = -1;
    protected int capacity;
    protected int threshold;
    protected float loadFactor = DEFAULT_LOAD_FACTOR;
    /**
     * Copied from HashMap
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private static final Logger logger = LogManager.getLogger(CircuitBreakingMap.class);
    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingMap(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        map = newInternalMap();
    }

    protected Map<K, V> newInternalMap() {
        Map<K, V> map = new HashMap<>();
        addToBreaker(RamUsageEstimator.sizeOfObject(map), true);
        return map;
    }

    /**
     * Return the size to reserve in CB, when the internal collection size changes.
     */
    protected long sizeToReserve() {
        if(size() <= threshold) {
            return capacity;
        }
        // Copy pasted from HashMap
        int newCapacity, newThreshold = 0;
        if (capacity > 0) {
            if (capacity >= MAXIMUM_CAPACITY) {
                threshold = Integer.MAX_VALUE;
                return capacity;
            } else if ((newCapacity = capacity << 1) < MAXIMUM_CAPACITY && capacity >= DEFAULT_INITIAL_CAPACITY) {
                newThreshold = threshold << 1; // double threshold
            }
        }
        else if (threshold > 0) {// initial capacity was placed in threshold
            newCapacity = threshold;
        } else {               // zero initial threshold signifies using defaults
            newCapacity = DEFAULT_INITIAL_CAPACITY;
            newThreshold = (int)(DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
        }

        if (newThreshold == 0) {
            float ft = (float)newCapacity * loadFactor;
            newThreshold = (newCapacity < MAXIMUM_CAPACITY && ft < (float)MAXIMUM_CAPACITY ?
                    (int)ft : Integer.MAX_VALUE);
        }
        this.threshold = newThreshold;
        return this.capacity = newCapacity;
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
        long newReservedSize = sizeToReserve();
        assert newReservedSize >= reservedSize : "Can only grow, not shrink";
        updateBreaker(newReservedSize);
    }

    protected void updateBreaker(long newReservedSize) {
        long sizeDiff = newReservedSize - reservedSize;
        // Since this method is called after collection already grew, update reservedSize even if breaking.
        reservedSize = newReservedSize;
        if (sizeDiff == 0) {
            return;
        }
        if (perElementSize == -1) {
            Optional<Entry<K, V>> optionalEntry = map.entrySet().stream().findAny();
            assert this.size() > 0 && optionalEntry.isPresent(): "Size should have changed from 0";
            Entry<K, V> entry = optionalEntry.get();
            perElementSize = RamUsageEstimator.shallowSizeOf(entry) + RamUsageEstimator.sizeOfObject(entry.getKey(), 0)
                    + RamUsageEstimator.sizeOfObject(entry.getValue(), 0);
        }
        // If it breaks, then the already created data will not be accounted for.
        // So we first add without breaking, and then check.
        addToBreaker(sizeDiff * perElementSize, false);
        addToBreaker(0, true);
    }
    @Override
    public void close() {
        map.clear();
        updateBreaker(0);
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
                capacity == that.capacity &&
                threshold == that.threshold &&
                loadFactor == that.loadFactor &&
                perElementSize == that.perElementSize &&
                Objects.equals(circuitBreaker, that.circuitBreaker) &&
                Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(circuitBreaker, map, requestBytesAdded, reservedSize, capacity, threshold, loadFactor, perElementSize);
    }
}
