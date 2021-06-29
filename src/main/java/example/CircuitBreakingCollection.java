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

    protected long elementSize(E e) {
        return RamUsageEstimator.sizeOfObject(e, 0) + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    protected void addToBreaker(E e) {
        addToBreaker(elementSize(e));
    }

    protected void removeFromBreaker(E e) {
        addToBreaker(-elementSize(e));
    }

//    protected void updateBreaker() {
//        long sizeDiff = collection.size() - prevSize;
//        prevSize = collection.size();
//        if (sizeDiff == 0) {
//            return;
//        }
//        if (perElementSize == -1) {
//            assert this.size() > 0 : "Size should have changed from 0";
//            perElementSize = RamUsageEstimator.sizeOfObject(collection.toArray()[0], 0) + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
//        }
//        addToBreaker(sizeDiff * perElementSize);
//    }
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
        boolean ret = collection.add(e);
        if (ret) {
            addToBreaker(e);
        }
        return ret;
    }

    @Override
    public boolean remove(Object o) {
        boolean ret = collection.remove(o);
        if (ret) {
            removeFromBreaker((E) o);
        }
        return ret;
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        return this.collection.containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        boolean ret = false;
        for (E e : collection) {
            ret = add(e) || ret;
        }
        return ret;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        boolean ret = false;
        for (Object o : collection) {
            ret = remove(o) || ret;
        }
        return ret;
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
