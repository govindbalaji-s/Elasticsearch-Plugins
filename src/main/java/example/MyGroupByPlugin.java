package example;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.*;

import static java.util.Collections.singletonList;


public class MyGroupByPlugin extends Plugin implements SearchPlugin, ScriptPlugin {
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
                if(code.equals("my-sum")) {
                    FieldScript.Factory factory = new MySumScriptFactory();
                    return scriptContext.factoryClazz.cast(factory);
                } else if(code.equals("my-join")) {
                    AggregationScript.Factory factory = new MyJoinScriptFactory();
                    return scriptContext.factoryClazz.cast(factory);
                }
                throw new IllegalArgumentException("unknown script name");
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

    public static ScriptDocValues safeGetScriptDocValues(Map<String, ScriptDocValues<?>> docLookup, String fieldName) {
        if (docLookup.containsKey(fieldName)) {
            return docLookup.get(fieldName);
        }
        return null;
    }
}
