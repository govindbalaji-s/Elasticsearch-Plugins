package example;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public class MySumScriptFactory implements FieldScript.Factory {
    public MySumScriptFactory() {
    }

    @Override
    public FieldScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup) {
        String sumField = XContentMapValues.nodeStringValue(params.get("sumField"));
        return leafReaderContext -> new MySumScript(sumField, params, searchLookup, leafReaderContext);
    }
}
