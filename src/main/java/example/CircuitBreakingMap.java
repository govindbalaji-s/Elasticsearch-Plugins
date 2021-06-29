package example;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.*;

public class CircuitBreakingMap<K, V> implements Map<K, V>, Releasable {
    private final CircuitBreaker circuitBreaker;
    private final Map<K, V> map;
    private long requestBytesAdded = 0;
    private long prevSize = 0;
    private long perElementSize = -1;
    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingMap(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        map = newInternalMap();
    }

    protected Map<K, V> newInternalMap() {
        Map<K, V> map = new HashMap<>();
        addToBreaker(RamUsageEstimator.sizeOfObject(map));
        return map;
    }

    protected void addToBreaker(long bytes) {
        if (bytes >= 0) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, "<CircuitBreakingList>");
        } else {
            circuitBreaker.addWithoutBreaking(bytes);
        }
        this.requestBytesAdded += bytes;
    }

    protected void updateBreaker() {
        long sizeDiff = map.size() - prevSize;
        prevSize = map.size();
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
        addToBreaker(sizeDiff * perElementSize);
    }
    @Override
    public void close() {
        map.clear();
        addToBreaker(-requestBytesAdded);
        requestBytesAdded = 0;
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
                prevSize == that.prevSize &&
                perElementSize == that.perElementSize &&
                Objects.equals(circuitBreaker, that.circuitBreaker) &&
                Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(circuitBreaker, map, requestBytesAdded, prevSize, perElementSize);
    }
}
