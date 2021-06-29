package example;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.Collection;
import java.util.Iterator;

public abstract class CircuitBreakingCollection<E> implements Collection<E>, Releasable {
    private final CircuitBreaker circuitBreaker;
    private final Collection<E> collection;
    private long requestBytesAdded = 0;
    private long prevSize = 0;
    private long perElementSize = -1;
    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingCollection(CircuitBreaker circuitBreaker) {
        super();
        this.circuitBreaker = circuitBreaker;
        collection = newInternalCollection();
    }

    protected abstract Collection<E> newInternalCollection();

    protected void addToBreaker(long bytes) {
        if (bytes >= 0) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, "<CircuitBreakingList>");
        } else {
            circuitBreaker.addWithoutBreaking(bytes);
        }
        this.requestBytesAdded += bytes;
    }

    protected void updateBreaker() {
        long sizeDiff = collection.size() - prevSize;
        prevSize = collection.size();
        if (sizeDiff == 0) {
            return;
        }
        if (perElementSize == -1) {
            assert this.size() > 0 : "Size should have changed from 0";
            perElementSize = RamUsageEstimator.sizeOfObject(collection.toArray()[0], 0) + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }
        addToBreaker(sizeDiff * perElementSize);
    }
    @Override
    public int size() {
        return collection.size();
    }

    @Override
    public boolean isEmpty() {
        return collection.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return collection.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return collection.iterator();
    }

    @Override
    public Object[] toArray() {
        return collection.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return collection.toArray(ts);
    }

    @Override
    public boolean add(E e) {
        try {
            return collection.add(e);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean remove(Object o) {
        try {
            return collection.remove(o);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        return this.collection.containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        try {
            return this.collection.addAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        try {
            return this.collection.removeAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        try {
            return this.collection.retainAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void clear() {
        try {
            collection.clear();
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void close() {
        collection.clear();
        addToBreaker(-requestBytesAdded);
        requestBytesAdded = 0;
    }
}
