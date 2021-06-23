package example;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.CircuitBreakerServicePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.*;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.*;
import java.util.function.Supplier;


public class MyGroupByPlugin extends Plugin implements SearchPlugin, ScriptPlugin, CircuitBreakerServicePlugin {
    Client esClient;
    CircuitBreakerService circuitBreakerService;
    BigArrays bigArrays;
//    public final static Cleaner cleaner = AccessController.doPrivileged((PrivilegedAction<Cleaner>) Cleaner::create);
    @Override
    public List<AggregationSpec> getAggregations() {
        return new ArrayList<AggregationSpec>(Arrays.asList(
                new AggregationSpec(MyGroupByBuilder.NAME, MyGroupByBuilder::new, MyGroupByBuilder.PARSER),
        new AggregationSpec(NoCBTermsAggregationBuilder.NAME, NoCBTermsAggregationBuilder::new, NoCBTermsAggregationBuilder.PARSER))
        );
    }

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new ScriptEngine() {
            static final String TYPE = "my-script";
            @Override
            public String getType() {
                return TYPE;
            }

            @Override
            public <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> scriptContext, Map<String, String> params) {
                ScriptFactory factory = null;
                switch(code) {
                    case "my-sum":
                        factory = new MySumScriptFactory();
                        break;
                    case "my-join":
                        factory = new MyJoinScriptFactory(esClient, circuitBreakerService);
                        break;
                    case "my-expand-init":
                        factory = new MemorySpamAgg.InitScriptFactory(circuitBreakerService, bigArrays);
                        break;
                    case "my-expand-map":
                        factory = new MemorySpamAgg.MapScriptFactory(circuitBreakerService, bigArrays);
                        break;
                    case "my-expand-combine":
                        factory = new MemorySpamAgg.CombineScriptFactory(circuitBreakerService, bigArrays);
                        break;
                    case "my-expand-reduce":
                        factory = new MemorySpamAgg.ReduceScriptFactory(circuitBreakerService, bigArrays);
                        break;
                    default:
                        throw new IllegalArgumentException("unknown script name");
                }
                return scriptContext.factoryClazz.cast(factory);
            }

            @Override
            public Set<ScriptContext<?>> getSupportedContexts() {
                Set<ScriptContext<?>> ret = new HashSet<ScriptContext<?>>();
                ret.add(AggregationScript.CONTEXT);
                ret.add(FieldScript.CONTEXT);
                return ret;
            }
        };
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.esClient = client;
        return Collections.emptyList();
    }

    public static ScriptDocValues safeGetScriptDocValues(Map<String, ScriptDocValues<?>> docLookup, String fieldName) {
        if (docLookup.containsKey(fieldName)) {
            return docLookup.get(fieldName);
        }
        return null;
    }

    @Override
    public void setCircuitBreakerService(CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
    }

    @Override
    public void setBigArrays(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }
}
