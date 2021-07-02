package example;

import org.elasticsearch.common.breaker.CircuitBreaker;

public class FinalizableCircuitBreakingMap<K, V> extends CircuitBreakingMap<K, V>{

    public FinalizableCircuitBreakingMap(CircuitBreaker circuitBreaker) {
        super(circuitBreaker);
    }

    public FinalizableCircuitBreakingMap(CircuitBreaker circuitBreaker, int initialCapacity, float loadFactor) {
        super(circuitBreaker, initialCapacity, loadFactor);
    }

    public FinalizableCircuitBreakingMap(CircuitBreaker circuitBreaker, int initialCapacity) {
        super(circuitBreaker, initialCapacity, DEFAULT_LOAD_FACTOR);
    }
    
    @Override
    public void finalize() {
        this.close();
    }
}