package example;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

public class NoCBTermsAggregationBuilder extends TermsAggregationBuilder {
    public static final String NAME = "nocb-terms";

    public static final ObjectParser<TermsAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, TermsAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

        PARSER.declareBoolean(TermsAggregationBuilder::showTermDocCountError,
                TermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR);

        PARSER.declareInt(TermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(TermsAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(TermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(TermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareString(TermsAggregationBuilder::executionHint, EXECUTION_HINT_FIELD_NAME);

        PARSER.declareField(TermsAggregationBuilder::collectMode,
                (p, c) -> Aggregator.SubAggCollectionMode.parse(p.text(), LoggingDeprecationHandler.INSTANCE),
                Aggregator.SubAggCollectionMode.KEY, ObjectParser.ValueType.STRING);

        PARSER.declareObjectArray(TermsAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p),
                TermsAggregationBuilder.ORDER_FIELD);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
                IncludeExclude::parseInclude, IncludeExclude.INCLUDE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
                IncludeExclude::parseExclude, IncludeExclude.EXCLUDE_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    public NoCBTermsAggregationBuilder(String name) {
        super(name);
    }

    protected NoCBTermsAggregationBuilder(TermsAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    public NoCBTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }
}
