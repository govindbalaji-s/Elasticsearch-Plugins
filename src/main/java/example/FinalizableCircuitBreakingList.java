package example;

import org.elasticsearch.common.breaker.CircuitBreaker;

public class FinalizableCircuitBreakingList<E> extends CircuitBreakingList<E>{
    public FinalizableCircuitBreakingList(CircuitBreaker circuitBreaker) {
        super(circuitBreaker);
    }

    @Override
    public void finalize() {
        this.close();
    }
}
