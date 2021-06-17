package example;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MySumScript extends FieldScript {
    String sumField;
    SearchLookup searchLookup;
    LeafReaderContext leafCtx;
    Set<Integer> readDocs = new HashSet<Integer>();
    double ans = 0;

    public MySumScript(String sumField, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext leafReaderContext) {
        super(params, searchLookup, leafReaderContext);
        this.sumField = sumField;
        this.searchLookup = searchLookup;
        this.leafCtx = leafReaderContext;
    }

    @Override
    public void setDocument(int docid) {
        super.setDocument(docid);
        if(!readDocs.contains(docid)) {
            fetch(docid);
            readDocs.add(docid);
        }
    }

    void fetch(int docid) {
        LeafSearchLookup lookup = searchLookup.getLeafSearchLookup(leafCtx);
        lookup.setDocument(docid);
        ScriptDocValues scriptDocValues = MyGroupByPlugin.safeGetScriptDocValues(lookup.doc(), sumField);
        ans = 0;
        for(int i = 0; i < scriptDocValues.size(); i++)
            ans += (double)scriptDocValues.get(i);
    }

    @Override
    public Object execute() {
        return ans;
    }
}
