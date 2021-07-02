package example;

import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.Collection;

public class FinalizableCircuitBreakingList<E> extends CircuitBreakingList<E>{
    public FinalizableCircuitBreakingList(CircuitBreaker circuitBreaker) {
        super(circuitBreaker);
    }

    public FinalizableCircuitBreakingList(CircuitBreaker circuitBreaker, int initialCapacity) {
        super(circuitBreaker, initialCapacity);
    }

    public FinalizableCircuitBreakingList(CircuitBreaker circuitBreaker, Collection<? extends E> collection) {
        super(circuitBreaker, collection);
    }

    @Override
    public void finalize() {
        this.close();
    }
}
