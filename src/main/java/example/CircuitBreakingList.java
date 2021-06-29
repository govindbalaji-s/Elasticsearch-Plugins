package example;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.*;

/**
 * A wrapper around a List<E> that updates a CircuitBreaker, every time the list's size changes.
 * This uses ArrayList<E> internally. Override newInternalList() to use other implementations of List<E>
 */
public class CircuitBreakingList<E> extends CircuitBreakingCollection<E> implements List<E> {

    List<E> list;

    public CircuitBreakingList(CircuitBreaker circuitBreaker) {
        super(circuitBreaker);
    }

    @Override
    protected Collection<E> newInternalCollection() {
        list = newInternalList();
        return list;
    }

    protected List<E> newInternalList() {
        List<E> list = new ArrayList<>();
        addToBreaker(RamUsageEstimator.sizeOfObject(list, 0));
        return list;
    }

    @Override
    public boolean addAll(int i, Collection<? extends E> collection) {
        return false;
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
}
