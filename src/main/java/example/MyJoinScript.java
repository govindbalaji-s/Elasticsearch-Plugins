package example;

//import org.apache.http.HttpHost;
//import org.apache.http.entity.ContentType;
//import org.apache.http.nio.entity.NStringEntity;
//import org.apache.http.util.EntityUtils;
//import org.apache.http.util.EntityUtils;
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
    Set<Integer> readDocs = new HashSet<Integer>();

    private static final Logger logger = LogManager.getLogger(MyJoinScript.class);
    static RestClient client;
//    static RestHighLevelClient client;
//    HttpClient client;

    public MyJoinScript(String fkField, String indexField, String valueField, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext leafReaderContext) {
        super(params, searchLookup, leafReaderContext);
        this.fkField = fkField;
        this.indexField = indexField;
        this.valueField = valueField;
        this.searchLookup = searchLookup;
        this.leafCtx = leafReaderContext;
        if(client == null) {
////             client =  new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http"))
////                        );
            client = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                   .build();
//            client = HttpClient.newBuilder()
//                    .version(HttpClient.Version.HTTP_2)
//                    .build();
        }
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
        ScriptDocValues scriptDocValues = MyGroupByPlugin.safeGetScriptDocValues(lookup.doc(), fkField);
//        ans = 0;
        List<String> fks = new LinkedList<String>();
        for(int i = 0; i < scriptDocValues.size(); i++)
            fks.add(scriptDocValues.get(i).toString());
        fetchExternal(fks);
    }

    private void fetchExternal(List<String> fks) {
//        MultiGetRequest request = new MultiGetRequest();
//        for(String fk : fks) {
//            request.add(new MultiGetRequest.Item(indexField, fk));
//        }

        Request request = new Request("GET", "/"+indexField+"/_mget");
//        HttpClient client = HttpClient.newBuilder()
//                    .version(HttpClient.Version.HTTP_2)
//                    .build();
        JSONArray ids = new JSONArray();
        for(String fk : fks) {
            ids.put(new JSONObject().put("_id", fk).put("_source", valueField));
        }
        String reqBody = new JSONObject().put("docs", ids).toString();
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create("http://localhost:9200"))
//                .header("Content-Type", "application/json")
//                .POST(HttpRequest.BodyPublishers.ofString(reqBody))
//                .build();
        request.setJsonEntity(reqBody);
        logger.info(indexField);
        logger.info(reqBody);
        SecurityManager sm = System.getSecurityManager();
        if(sm != null) {
            sm.checkPermission(new SocketPermission("localhost:9200",
                    "connect,resolve"));
        }
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
//            MultiGetResponse rsp = client.mget(request, RequestOptions.DEFAULT);
//            for(MultiGetItemResponse irsp : rsp.getResponses()) {
//                GetResponse grsp = irsp.getResponse();
//                if(grsp.isExists() && grsp.getSourceAsMap().containsKey(valueField)) {
//                    values.add(grsp.getSourceAsMap().get(valueField));
//                }
//            }
            } catch (Exception e) {
                throw new ElasticsearchException("can't fetch external" + e.getMessage());
            }
            return null;
        });
    }

    private static String makeRequest(List<String> fks) {
        String ret = "{\"docs\": [";
        int i = 0;
        for(String fk : fks) {
            ret += "{\"_id\": \""+fk+"\"}";
            if(i < fks.size())
                ret += ",";
            i++;
        }
        ret += "]}";
        return ret;
    }

    @Override
    public Object execute() {
        try {
            return values;
        } finally{
            values.clear();
        }
    }
}
