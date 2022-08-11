/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A field mapper for Flat objects. This mapper ... TODO: add more description
 *
 * @opensearch.internal
 */
public class FlatObjectFieldMapper extends DynamicKeyFieldMapper {

    public static final String CONTENT_TYPE = "flat";


    /**
     * Default parameters
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * The flat field for the field mapper
     *
     * @opensearch.internal
     */
    public static class FlatField extends Field {

        public FlatField(String field, BytesRef term, FieldType ft) {
            super(field, term, ft);
        }
    }

    private static FlatObjectFieldMapper toType(FieldMapper fieldMapper) {
        return (FlatObjectFieldMapper) fieldMapper;
    }

    /**
     * The builder for the field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final ParametrizedFieldMapper.Parameter<Boolean> indexed = ParametrizedFieldMapper.Parameter.indexParam(m -> toType(m).indexed, true);
        private final ParametrizedFieldMapper.Parameter<Boolean> hasDocValues = ParametrizedFieldMapper.Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final ParametrizedFieldMapper.Parameter<String> nullValue = ParametrizedFieldMapper.Parameter.stringParam("null_value", false, m -> toType(m).nullValue, null)
            .acceptsNull();
/*
        private final ParametrizedFieldMapper.Parameter<Boolean> eagerGlobalOrdinals = ParametrizedFieldMapper.Parameter.boolParam(
            "eager_global_ordinals",
            true,
            m -> toType(m).eagerGlobalOrdinals,
            false
        );
        private final ParametrizedFieldMapper.Parameter<Boolean> splitQueriesOnWhitespace = ParametrizedFieldMapper.Parameter.boolParam(
            "split_queries_on_whitespace",
            true,
            m -> toType(m).splitQueriesOnWhitespace,
            false
        );*/

        private final ParametrizedFieldMapper.Parameter<Map<String, String>> meta = ParametrizedFieldMapper.Parameter.metaParam();
        private final ParametrizedFieldMapper.Parameter<Float> boost = ParametrizedFieldMapper.Parameter.boostParam();

        private final IndexAnalyzers indexAnalyzers;

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexAnalyzers = indexAnalyzers;
        }

        public Builder(String name) {
            this(name, null);
        }


        FlatObjectFieldMapper.Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public FlatObjectFieldMapper.Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }


        @Override
        protected List<ParametrizedFieldMapper.Parameter<?>> getParameters() {
            return Arrays.asList(
                indexed,
                hasDocValues,
                nullValue,
                boost,
                meta
            );
        }

        private FlatFieldType buildFieldType(BuilderContext context, FieldType fieldType) {
           /* TODO: maybe add later
           NamedAnalyzer normalizer = Lucene.KEYWORD_ANALYZER;
            NamedAnalyzer searchAnalyzer = Lucene.KEYWORD_ANALYZER;
            String normalizerName = this.normalizer.getValue();
            if (Objects.equals(normalizerName, "default") == false) {
                assert indexAnalyzers != null;
                normalizer = indexAnalyzers.getNormalizer(normalizerName);
                if (normalizer == null) {
                    throw new MapperParsingException("normalizer [" + normalizerName + "] not found for field [" + name + "]");
                }
                if (splitQueriesOnWhitespace.getValue()) {
                    searchAnalyzer = indexAnalyzers.getWhitespaceNormalizer(normalizerName);
                } else {
                    searchAnalyzer = normalizer;
                }
            } else if (splitQueriesOnWhitespace.getValue()) {
                searchAnalyzer = Lucene.WHITESPACE_ANALYZER;
            }*/
            return new FlatFieldType(buildFullName(context), fieldType, Lucene.KEYWORD_ANALYZER, this);
        }

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            return null;
        }
    }


    public static final TypeParser PARSER = new TypeParser((n, c) -> new KeywordFieldMapper.Builder(n, c.getIndexAnalyzers()));

    public static final class FlatFieldType extends StringFieldType {

        public FlatFieldType(String name, FieldType fieldType, NamedAnalyzer normalizer, FlatObjectFieldMapper.Builder builder) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                fieldType.stored(),
                builder.hasDocValues.getValue(),
                TextSearchInfo.SIMPLE_MATCH_ONLY,
                builder.meta.getValue()
            );
            setIndexAnalyzer(normalizer);
            setBoost(builder.boost.getValue());
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return null;
        }

        @Override
        public String typeName() {
            return null;
        }
    }

    private final boolean indexed;
    private final boolean hasDocValues;
    private final String nullValue;
    private final FieldType fieldType;

    protected FlatObjectFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        CopyTo copyTo,
        FlatObjectFieldMapper.Builder builder
    ) {
        super(simpleName, mappedFieldType, copyTo);
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.fieldType = fieldType;

    }

    @Override
    public MappedFieldType keyedFieldType(String key) {
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {

    }

    @Override
    protected String contentType() {
        return null;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return null;
    }
}
