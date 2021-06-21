package example;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.*;

public class MemorySpamAgg{
    // Assuming each value takes <= 100 bytes
    static final long DEFAULT_WEIGHT = 100;
    static public class InitScriptFactory implements ScriptedMetricAggContexts.InitScript.Factory {
        CircuitBreaker circuitBreaker;
        InitScriptFactory(CircuitBreakerService cbs) {
            circuitBreaker = cbs.getBreaker(CircuitBreaker.REQUEST);
        }

        @Override
        public ScriptedMetricAggContexts.InitScript newInstance(Map<String, Object> params, Map<String, Object> state) {
            return new ScriptedMetricAggContexts.InitScript(params, state) {
                @Override
                public void execute() {
                    /*
                    state["vals"] is the ArrayList of all values of params["field"] field in this shard.
                    state["vals-size"] - size of state["vals"], so that it's size can be known without referencing
                                          the ArrayList. This is needed to reclaim CB.
                     */
                    state.put("vals", new ArrayList<Object>(){
                        @Override
                        public void finalize() {
                            long sz = XContentMapValues.nodeIntegerValue(state.get("vals-size"), 0);
                            circuitBreaker.addWithoutBreaking(-sz * DEFAULT_WEIGHT);
                            state.put("vals-size", 0L);
                        }
                    });
                    state.put("vals-size", 0L);
                }
            };
        }
    }

    static public class MapScriptFactory implements ScriptedMetricAggContexts.MapScript.Factory {
        CircuitBreaker circuitBreaker;
        MapScriptFactory(CircuitBreakerService cbs) {
            circuitBreaker = cbs.getBreaker(CircuitBreaker.REQUEST);
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
                    ArrayList<Object> vals = (ArrayList<Object>) state.get("vals");
                    for(Object value : scriptDocValues) {
                        circuitBreaker.addEstimateBytesAndMaybeBreak(DEFAULT_WEIGHT, "my-script/my-expand-map");
                        long sz = (long)state.get("vals-size");
                        state.put("vals-size", sz+1);
                        vals.addAll(expand(value));
                    }
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
    static public class CombineScriptFactory implements ScriptedMetricAggContexts.CombineScript.Factory {
        @Override
        public ScriptedMetricAggContexts.CombineScript newInstance(Map<String, Object> params, Map<String, Object> state) {
            return new ScriptedMetricAggContexts.CombineScript(params, state) {
                @Override
                public Object execute() {
                    return state.get("vals");
                }
            };
        }
    }
    static public class ReduceScriptFactory implements ScriptedMetricAggContexts.ReduceScript.Factory {
        CircuitBreaker circuitBreaker;
        ReduceScriptFactory(CircuitBreakerService cbs) {
            circuitBreaker = cbs.getBreaker(CircuitBreaker.REQUEST);
        }
        @Override
        public ScriptedMetricAggContexts.ReduceScript newInstance(Map<String, Object> params, List<Object> states) {
             return new ScriptedMetricAggContexts.ReduceScript(params, states) {
                 @Override
                 public Object execute() {
                     if(states.isEmpty())
                         return new ArrayList<>();
                     final long[] size = {0L};
                     List<Object> ret = new ArrayList(){
                         @Override
                         public void finalize() {
                             circuitBreaker.addWithoutBreaking(-size[0] * DEFAULT_WEIGHT);
                         }
                     };
                     for(Object state : states) {
                         List<Object> vals = (ArrayList<Object>) state;
                         circuitBreaker.addEstimateBytesAndMaybeBreak(vals.size() * DEFAULT_WEIGHT, "my-script/my-expand-reduce");
                         size[0] += vals.size();
                         ret.addAll(vals);
                     }
                     return ret;
                 }
             };
        }
    }
}
