package example;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;

import java.util.*;

/**
 * A wrapper around a List<E> that updates a CircuitBreaker, every time the list's size changes.
 * This uses ArrayList<E> internally. Override newInternalList() to use other implementations of List<E>
 */
public class CircuitBreakingList<E> implements List<E>, Releasable {
    CircuitBreaker circuitBreaker;
    List<E> list;
    long requestBytesAdded = 0;
    long prevSize = 0;
    long perElementSize = -1;
    // bytes for the above fields themselves aren't counted.

    public CircuitBreakingList(CircuitBreaker circuitBreaker) {
        super();
        this.circuitBreaker = circuitBreaker;
        list = newInternalList();
    }

    protected List<E> newInternalList() {
        List<E> list = new ArrayList<>();
        addToBreaker(RamUsageEstimator.sizeOfObject(list, 0));
        return list;
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
        long sizeDiff = list.size() - prevSize;
        prevSize = list.size();
        if (sizeDiff == 0) {
            return;
        }
        if (perElementSize == -1) {
            perElementSize = RamUsageEstimator.sizeOfObject(list.get(0), 0) + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }
        addToBreaker(sizeDiff * perElementSize);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return list.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return list.iterator();
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return list.toArray(ts);
    }

    @Override
    public boolean add(E e) {
        try {
            return list.add(e);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean remove(Object o) {
        try {
            return list.remove(o);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        return list.containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        try {
            return list.addAll(collection);
        } finally {
            updateBreaker();
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
    public boolean removeAll(Collection<?> collection) {
        try {
            return list.removeAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        try {
            return list.removeAll(collection);
        } finally {
            updateBreaker();
        }
    }

    @Override
    public void clear() {
        try {
            list.clear();
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
    public void close() {
        list.clear();
        addToBreaker(-requestBytesAdded);
        requestBytesAdded = 0;
    }
}
