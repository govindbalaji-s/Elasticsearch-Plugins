package example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;

public abstract class CircuitBreakingCollection<E> implements Collection<E>, Releasable {
    private final CircuitBreaker circuitBreaker;
    protected final Collection<E> collection;
    private long requestBytesAdded = 0;
    protected long reservedSize = 0;
    private long perElementSize = -1;
    private static final Logger logger = LogManager.getLogger(CircuitBreakingCollection.class);
    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingCollection(CircuitBreaker circuitBreaker) {
        super();
        this.circuitBreaker = circuitBreaker;
        collection = newInternalCollection();
    }

    protected abstract Collection<E> newInternalCollection();

    /**
     * Return the size to reserve in CB, when the internal collection size changes.
     */
    protected abstract long sizeToReserve();

    protected void addToBreaker(long bytes, boolean checkBreaker) {
        if (bytes >= 0 && checkBreaker) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, "<CircuitBreakingList>");
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
            assert this.size() > 0 : "Size should have changed from 0";
            perElementSize = RamUsageEstimator.sizeOfObject(collection.toArray()[0], 0) + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }
        // If it breaks, then the already created data will not be accounted for.
        // So we first add without breaking, and then check.
        addToBreaker(sizeDiff * perElementSize, false);
        addToBreaker(0, true);
    }

    public void shrinkReservationToSize() {
        updateBreaker(size());
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
    public boolean removeIf(Predicate<? super E> filter) {
        try {
            return collection.removeIf(filter);
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
            logger.info("I am reserving none from " + reservedSize);
            updateBreaker(0);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakingCollection<?> that = (CircuitBreakingCollection<?>) o;
        return requestBytesAdded == that.requestBytesAdded &&
                reservedSize == that.reservedSize &&
                perElementSize == that.perElementSize &&
                Objects.equals(circuitBreaker, that.circuitBreaker) &&
                Objects.equals(collection, that.collection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(circuitBreaker, collection, requestBytesAdded, reservedSize, perElementSize);
    }

    @Override
    public void close() {
        clear();
    }
}
