package example;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public class MyJoinScriptFactory implements AggregationScript.Factory {
    public MyJoinScriptFactory() {
    }

    @Override
    public AggregationScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup) {
        String fkField = XContentMapValues.nodeStringValue(params.get("fkField"));
        String indexField = XContentMapValues.nodeStringValue(params.get("indexField"));
        String valueField = XContentMapValues.nodeStringValue(params.get("valueField"));
        return new AggregationScript.LeafFactory() {
            @Override
            public AggregationScript newInstance(LeafReaderContext leafReaderContext) throws IOException {
                return new MyJoinScript(fkField, indexField, valueField, params, searchLookup, leafReaderContext);
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
    }
}
