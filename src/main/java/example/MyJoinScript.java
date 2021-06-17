package example;

import org.apache.http.HttpHost;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.*;

public class MyJoinScript extends AggregationScript {
    String fkField, indexField, valueField;
    SearchLookup searchLookup;
    LeafReaderContext leafCtx;
    List<Object> values = new LinkedList<>();
    Set<Integer> readDocs = new HashSet<Integer>();
    static RestHighLevelClient client;

    public MyJoinScript(String fkField, String indexField, String valueField, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext leafReaderContext) {
        super(params, searchLookup, leafReaderContext);
        this.fkField = fkField;
        this.indexField = indexField;
        this.valueField = valueField;
        this.searchLookup = searchLookup;
        this.leafCtx = leafReaderContext;
        if(client == null)
             client =  new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http"))
                        );
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
            fks.add((String)scriptDocValues.get(i));
        fetchExternal(fks);
    }

    private void fetchExternal(List<String> fks) {
        MultiGetRequest request = new MultiGetRequest();
        for(String fk : fks) {
            request.add(new MultiGetRequest.Item(indexField, fk));
        }

        try {
            MultiGetResponse rsp = client.mget(request, RequestOptions.DEFAULT);
            for(MultiGetItemResponse irsp : rsp.getResponses()) {
                GetResponse grsp = irsp.getResponse();
                if(grsp.isExists() && grsp.getSourceAsMap().containsKey(valueField)) {
                    values.add(grsp.getSourceAsMap().get(valueField));
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchException("can't fetch external");
        }
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