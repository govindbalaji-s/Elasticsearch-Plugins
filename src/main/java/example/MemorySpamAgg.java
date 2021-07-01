package example;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.*;

public class MemorySpamAgg {

    static protected class MemorySpamAggScriptFactory {
        protected CircuitBreaker circuitBreaker;
        protected BigArrays bigArrays;

        protected MemorySpamAggScriptFactory(CircuitBreakerService circuitBreakerService, BigArrays bigArrays) {
            circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
            this.bigArrays = bigArrays;
        }
    }

    static public class InitScriptFactory extends MemorySpamAggScriptFactory implements ScriptedMetricAggContexts.InitScript.Factory {

        InitScriptFactory(CircuitBreakerService circuitBreakerService, BigArrays bigArrays) {
            super(circuitBreakerService, bigArrays);
        }

        @Override
        public ScriptedMetricAggContexts.InitScript newInstance(Map<String, Object> params, Map<String, Object> state) {
            return new ScriptedMetricAggContexts.InitScript(params, state) {
                @Override
                public void execute() {
                    /*
                    state["vals"] is the ArrayList of all values of params["field"] field in this shard.
                     */
                    List<Object> vals = new FinalizableCircuitBreakingList<>(circuitBreaker);
                    state.put("vals", vals);
                }
            };
        }
    }

    static public class MapScriptFactory extends MemorySpamAggScriptFactory implements ScriptedMetricAggContexts.MapScript.Factory {

        MapScriptFactory(CircuitBreakerService circuitBreakerService, BigArrays bigArrays) {
            super(circuitBreakerService, bigArrays);
        }

        @Override
        public ScriptedMetricAggContexts.MapScript.LeafFactory newFactory(Map<String, Object> params, Map<String, Object> state, SearchLookup searchLookup) {

            return leafReaderContext -> new ScriptedMetricAggContexts.MapScript(params, state, searchLookup, leafReaderContext) {
                final LeafSearchLookup lookup = searchLookup.getLeafSearchLookup(leafReaderContext);

                @Override
                public void setDocument(int docid) {
                    super.setDocument(docid);
                    lookup.setDocument(docid);
                }

                @Override
                public void execute() {
                    ScriptDocValues scriptDocValues = MyGroupByPlugin.safeGetScriptDocValues(
                            lookup.doc(),
                            XContentMapValues.nodeStringValue(params.get("field"))
                    );
                    List<Object> list = (List<Object>) state.get("vals");
                    list.addAll(scriptDocValues);
                }
            };
        }

        /**
         * If required can return a repeated array to increase memory usage.
         * Right now, the value is repeated only once.
         */
        private Collection<?> expand(Object value) {
            return Collections.singleton(value);
        }
    }

    static public class CombineScriptFactory extends MemorySpamAggScriptFactory implements ScriptedMetricAggContexts.CombineScript.Factory {

       CombineScriptFactory(CircuitBreakerService circuitBreakerService, BigArrays bigArrays) {
            super(circuitBreakerService, bigArrays);
        }

        @Override
        public ScriptedMetricAggContexts.CombineScript newInstance(Map<String, Object> params, Map<String, Object> state) {
            return new ScriptedMetricAggContexts.CombineScript(params, state) {
                @Override
                public Object execute() {
                    CircuitBreakingList<Object> list = (CircuitBreakingList<Object>) state.get("vals");
                    list.shrinkReservationToSize();
                    return list;
                }
            };
        }
    }

    static public class ReduceScriptFactory extends MemorySpamAggScriptFactory implements ScriptedMetricAggContexts.ReduceScript.Factory {

        ReduceScriptFactory(CircuitBreakerService circuitBreakerService, BigArrays bigArrays) {
            super(circuitBreakerService, bigArrays);
        }

        @Override
        public ScriptedMetricAggContexts.ReduceScript newInstance(Map<String, Object> params, List<Object> states) {
            return new ScriptedMetricAggContexts.ReduceScript(params, states) {
                @Override
                public Object execute() {
                    if (states.isEmpty())
                        return new FinalizableCircuitBreakingList<Object>(circuitBreaker);
                    CircuitBreakingList<Object> ret = new FinalizableCircuitBreakingList<>(circuitBreaker);
                    for(Object state: states) {
                        List<Object> vals = (List<Object>) state;
                        ret.addAll(vals);
                    }
                    ret.shrinkReservationToSize();
                    return ret;
                }
            };
        }
    }
}
