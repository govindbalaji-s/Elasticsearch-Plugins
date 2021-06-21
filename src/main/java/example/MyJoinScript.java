package example;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.lookup.EnvironmentLookup;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.SocketPermission;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

public class MyJoinScript extends AggregationScript {
    String fkField, indexField, valueField;
    SearchLookup searchLookup;
    LeafReaderContext leafCtx;
//    List<Object> values;
    List<String> fks;
    List<Object> values = new LinkedList<>();
//    BigArrays bigArrays;
    Client clt;
    CircuitBreaker requestCB;

    private static final Logger logger = LogManager.getLogger(MyJoinScript.class);
    static RestClient client;
    FetchSourceContext fetchSourceContext;

    public MyJoinScript(Client clt, CircuitBreaker requestCB, String fkField, String indexField, String valueField, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext leafReaderContext) {
        super(params, searchLookup, leafReaderContext);
        this.clt = clt;
        this.requestCB = requestCB;
        this.fkField = fkField;
        this.indexField = indexField;
        this.valueField = valueField;
        this.searchLookup = searchLookup;
        this.leafCtx = leafReaderContext;
        if(client == null) {
            client = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                   .build();
        }
        fetchSourceContext = new FetchSourceContext(true, new String[]{valueField}, Strings.EMPTY_ARRAY);

//        BigArrays.NON_RECYCLING_INSTANCE.b
//        bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
//        values = bigArrays.newObjectArray(2)
    }

    @Override
    public void setDocument(int docid) {
        super.setDocument(docid);
        fetch(docid);
    }

    /**
     * Populate values list from external index's documents corresponding to internal's docid
     * @param docid
     */
    void fetch(int docid) {
        // The outer "terms" aggregator estimates 5kB per bucket.
        // Estimate the same amount more.
//        requestCB.addEstimateBytesAndMaybeBreak(2*AggregatorBase.DEFAULT_WEIGHT, "<script my-script>");

        LeafSearchLookup lookup = searchLookup.getLeafSearchLookup(leafCtx);
        lookup.setDocument(docid);
        ScriptDocValues scriptDocValues = MyGroupByPlugin.safeGetScriptDocValues(lookup.doc(), fkField);
        fks = new LinkedList<String>();
        values.clear();
        for(int i = 0; i < scriptDocValues.size(); i++)
            fks.add(scriptDocValues.get(i).toString());
    }

    /**
     * Fetch valueField from documents in external index, whose id is in fks
     */
    private Object fetchExternal() {
        MultiGetRequest request = new MultiGetRequest();
        for(String fk : fks) {
            request.add(new MultiGetRequest.Item(indexField, fk).fetchSourceContext(fetchSourceContext));
        }
        List<Object> values = new ArrayList<>(2);
            try {
                MultiGetResponse response = clt.multiGet(request).actionGet();
                for(MultiGetItemResponse irsp : response.getResponses()) {
                    GetResponse grsp = irsp.getResponse();
                    if(grsp.isExists() && grsp.getSourceAsMap() != null && grsp.getSourceAsMap().containsKey(valueField)) {
                        values.add(grsp.getSourceAsMap().get(valueField));
                    }
                }
            } catch (Exception e) {
                throw new ElasticsearchException("can't fetch external" + e.getMessage(), e );
            }
        return values;
    }

    /**
     * Make _mget Request JSON string
     * @param fks list of _id
     * @return request string
     */
    private String makeMultiGetRequest(List<String> fks) {
//        BigArrays.NON_RECYCLING_INSTANCE.
        JSONArray ids = new JSONArray();
        for(String fk : fks) {
            ids.put(new JSONObject().put("_id", fk));
        }
        return new JSONObject().put("docs", ids).toString();
    }

    @Override
    public Object execute() {
        try {
            return fetchExternal();
        }
        finally {
//            requestCB.addWithoutBreaking(-2*AggregatorBase.DEFAULT_WEIGHT);
        }
    }
}
