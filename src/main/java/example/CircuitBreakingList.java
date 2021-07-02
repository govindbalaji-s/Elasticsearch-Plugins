package example;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.*;

/**
 * A wrapper around a List<E> that updates a CircuitBreaker, every time the list's size changes.
 * This uses ArrayList<E> internally. Override newInternalList() to use other implementations of List<E>
 */
public class CircuitBreakingList<E> extends CircuitBreakingCollection<E> implements List<E> {

    List<E> list;
    private static final int DEFAULT_CAPACITY = -1;
    int capacity = DEFAULT_CAPACITY;

    public CircuitBreakingList(CircuitBreaker circuitBreaker) {
        super(circuitBreaker);
    }

    public CircuitBreakingList(CircuitBreaker circuitBreaker, int initialCapacity) {
        super(circuitBreaker);
        if (capacity < 0) {
            throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
        }
        capacity = initialCapacity;
    }

    public CircuitBreakingList(CircuitBreaker circuitBreaker, Collection<? extends E> collection) {
        this(circuitBreaker, collection.size());
        addAll(collection);
    }

    @Override
    protected Collection<E> newInternalCollection() {
        list = newInternalList();
        return list;
    }

    @Override
    protected long sizeToReserve() {
        if (size() <= capacity) {
            return capacity;
        }
        // Copy pasted from ArrayList.java(JBR - 11) so that capacity grows same as ArrayList's internal capacity
        int minCapacity = size();
        if (capacity == DEFAULT_CAPACITY) {
            capacity = Math.max(10, minCapacity);
        } else {
            int newCapacity = capacity + (capacity >> 1);
            if (newCapacity - minCapacity <= 0) {
                capacity = minCapacity;
            } else {
                capacity = newCapacity - 2147483639 <= 0 ? newCapacity : (minCapacity > 2147483639 ? 2147483647 : 2147483639);
            }
        }
        return capacity;
    }

    protected List<E> newInternalList() {
        List<E> list;
        if (capacity == DEFAULT_CAPACITY) {
            list = new ArrayList<>();
        } else {
            list = new ArrayList<>(capacity);
        }
        addToBreaker(RamUsageEstimator.sizeOfObject(list, 0), false);
        return list;
    }

    public void shrinkReservationToSize() {
        if (list instanceof ArrayList) {
            ((ArrayList<E>) list).trimToSize();
            updateBreaker(size());
        } else {
            throw new UnsupportedOperationException("Can not shrink internal list");
        }
    }

    @Override
    public boolean addAll(int i, Collection<? extends E> collection) {
        try {
            return list.addAll(i, collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public E get(int i) {
        return list.get(i);
    }

    @Override
    public E set(int i, E e) {
        return list.set(i, e);
    }

    @Override
    public void add(int i, E e) {
        try {
            list.add(i, e);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public E remove(int i) {
        try {
            return list.remove(i);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public int indexOf(Object o) {
        return list.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return list.lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return list.listIterator();
    }

    @Override
    public ListIterator<E> listIterator(int i) {
        return list.listIterator(i);
    }

    @Override
    public List<E> subList(int i, int i1) {
        return list.subList(i, i1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CircuitBreakingList<?> that = (CircuitBreakingList<?>) o;
        return Objects.equals(list, that.list)
                && Objects.equals(capacity, that.capacity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), list, capacity);
    }
}
