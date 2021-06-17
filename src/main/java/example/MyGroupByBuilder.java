package example;
//import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class MyGroupByBuilder extends AbstractAggregationBuilder<MyGroupByBuilder> {
    public static String NAME = "my-group-by";
    String name;
    private TermsAggregationBuilder termsAggregationBuilder;
//    private SumAggregationBuilder sumAggregationBuilder;

    public MyGroupByBuilder(TermsAggregationBuilder tab, /*SumAggregationBuilder sab, */String name) {
        super(name);
        this.name = name;
        termsAggregationBuilder = tab;
//        sumAggregationBuilder = sab;
//        tab.subAggregation(sab);
    }

    protected MyGroupByBuilder(MyGroupByBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.termsAggregationBuilder = clone.termsAggregationBuilder;
//        this.sumAggregationBuilder = clone.sumAggregationBuilder;
    }

    protected MyGroupByBuilder(StreamInput in) throws IOException {
        super(in);
        termsAggregationBuilder = in.readNamedWriteable(TermsAggregationBuilder.class);
//        sumAggregationBuilder = in.readNamedWriteable(SumAggregationBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeNamedWriteable(termsAggregationBuilder);
//        streamOutput.writeNamedWriteable(sumAggregationBuilder);
    }

    @Override
    //TODOOOOOO
    protected AggregatorFactory doBuild(AggregationContext aggregationContext, AggregatorFactory parent, AggregatorFactories.Builder builder) throws IOException {
        return termsAggregationBuilder.build(aggregationContext, parent);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        termsAggregationBuilder.toXContent(xContentBuilder, params);
        return xContentBuilder;
    }

    public static final ConstructingObjectParser<MyGroupByBuilder, String> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new MyGroupByBuilder((TermsAggregationBuilder) args[0], "group"));
    static {
        PARSER.declareObject(constructorArg(), TermsAggregationBuilder.PARSER, new ParseField("group"));
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder builder, Map<String, Object> map) {
        return new MyGroupByBuilder(this, builder, map);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
