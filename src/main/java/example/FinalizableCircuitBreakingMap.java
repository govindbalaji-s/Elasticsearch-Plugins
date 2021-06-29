package example;

import org.elasticsearch.common.breaker.CircuitBreaker;

public class FinalizableCircuitBreakingMap<K, V> extends CircuitBreakingMap<K, V>{
    public FinalizableCircuitBreakingMap(CircuitBreaker circuitBreaker) {
        super(circuitBreaker);
    }

    @Override
    public void finalize() {
        this.close();
    }
}
