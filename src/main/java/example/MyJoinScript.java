package example;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.*;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AggregationScript;
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
    List<Object> values = new LinkedList<>();

    private static final Logger logger = LogManager.getLogger(MyJoinScript.class);
    static RestClient client;

    public MyJoinScript(String fkField, String indexField, String valueField, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext leafReaderContext) {
        super(params, searchLookup, leafReaderContext);
        this.fkField = fkField;
        this.indexField = indexField;
        this.valueField = valueField;
        this.searchLookup = searchLookup;
        this.leafCtx = leafReaderContext;
        if(client == null) {
            client = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                   .build();
        }
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
        LeafSearchLookup lookup = searchLookup.getLeafSearchLookup(leafCtx);
        lookup.setDocument(docid);
        ScriptDocValues scriptDocValues = MyGroupByPlugin.safeGetScriptDocValues(lookup.doc(), fkField);
        List<String> fks = new LinkedList<String>();
        values.clear();
        for(int i = 0; i < scriptDocValues.size(); i++)
            fks.add(scriptDocValues.get(i).toString());
        fetchExternal(fks);
    }

    /**
     * Fetch valueField from documents in external index, whose id is in fks
     * @param fks list of id
     */
    private void fetchExternal(List<String> fks) {
        Request request = new Request("GET", "/"+indexField+"/_mget");
        String reqBody = makeMultiGetRequest(fks);
        request.setJsonEntity(reqBody);

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                Response response = client.performRequest(request);
                JSONObject responseBody = new JSONObject(EntityUtils.toString(response.getEntity()));
                JSONArray docs = responseBody.getJSONArray("docs");

                for(int i = 0; i < docs.length(); i++) {
                    values.add(docs.getJSONObject(i)
                            .getJSONObject("_source")
                            .getString(valueField));

                }
            } catch (Exception e) {
                throw new ElasticsearchException("can't fetch external" + e.getMessage(), e );
            }
            return null;
        });
    }

    /**
     * Make _mget Request JSON string
     * @param fks list of _id
     * @return request string
     */
    private String makeMultiGetRequest(List<String> fks) {
        JSONArray ids = new JSONArray();
        for(String fk : fks) {
            ids.put(new JSONObject().put("_id", fk).put("_source", valueField));
        }
        return new JSONObject().put("docs", ids).toString();
    }

    @Override
    public Object execute() {
        return values;
    }
}
