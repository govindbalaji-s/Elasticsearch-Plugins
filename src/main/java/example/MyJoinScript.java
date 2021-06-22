package example;

import org.apache.http.HttpHost;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MyJoinScript extends AggregationScript {
    private String fkField, indexField, valueField;
    private SearchLookup searchLookup;
    private LeafReaderContext leafReaderContext;
    //    List<Object> values;
    private List<String> fks;
    List<Object> values = new LinkedList<>();
    //    BigArrays bigArrays;
    private  Client esClient;
    CircuitBreaker requestCB;

    static RestClient client;
    FetchSourceContext fetchSourceContext;

    public MyJoinScript(Client esClient, CircuitBreaker requestCB, String fkField, String indexField, String valueField, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext leafReaderContext) {
        super(params, searchLookup, leafReaderContext);
        this.esClient = esClient;
        this.requestCB = requestCB;
        this.fkField = fkField;
        this.indexField = indexField;
        this.valueField = valueField;
        this.searchLookup = searchLookup;
        this.leafReaderContext = leafReaderContext;
        if (client == null) {
            client = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                    .build();
        }
        fetchSourceContext = new FetchSourceContext(true, new String[]{valueField}, Strings.EMPTY_ARRAY);
    }

    @Override
    public void setDocument(int docid) {
        super.setDocument(docid);
        fetch(docid);
    }

    /**
     * Populate values list from external index's documents corresponding to internal's docId
     *
     * @param docId
     */
    void fetch(int docId) {
        // The outer "terms" aggregator estimates 5kB per bucket.
        // Estimate the same amount more.
//        requestCB.addEstimateBytesAndMaybeBreak(2*AggregatorBase.DEFAULT_WEIGHT, "<script my-script>");

        LeafSearchLookup lookup = searchLookup.getLeafSearchLookup(leafReaderContext);
        lookup.setDocument(docId);
        ScriptDocValues scriptDocValues = MyGroupByPlugin.safeGetScriptDocValues(lookup.doc(), fkField);
        if(scriptDocValues == null) {
            return;
        }
        fks = new LinkedList<>();
        values.clear();
        for (int i = 0; i < scriptDocValues.size(); i++)
            fks.add(scriptDocValues.get(i).toString());
    }

    /**
     * Fetch valueField from documents in external index, whose id is in fks
     */
    private Object fetchExternal() {
        MultiGetRequest request = new MultiGetRequest();
        for (String fk : fks) {
            request.add(new MultiGetRequest.Item(indexField, fk).fetchSourceContext(fetchSourceContext));
        }
        List<Object> values = new ArrayList<>(2);
        try {
            MultiGetResponse response = esClient.multiGet(request).actionGet();
            for (MultiGetItemResponse multiGetItemResponse : response.getResponses()) {
                GetResponse getResponse = multiGetItemResponse.getResponse();
                if (getResponse.isExists() && getResponse.getSourceAsMap() != null && getResponse.getSourceAsMap().containsKey(valueField)) {
                    values.add(getResponse.getSourceAsMap().get(valueField));
                }
            }
        } catch (Exception e) {
            throw new ElasticsearchException("can't fetch external" + e.getMessage(), e);
        }
        return values;
    }

    @Override
    public Object execute() {
        try {
            return fetchExternal();
        } finally {
//            requestCB.addWithoutBreaking(-2*AggregatorBase.DEFAULT_WEIGHT);
        }
    }
}
