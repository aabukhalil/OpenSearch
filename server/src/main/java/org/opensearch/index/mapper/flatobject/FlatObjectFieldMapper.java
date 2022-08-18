/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.mapper.*;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.*;

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
        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> toType(m).nullValue, null)
            .acceptsNull();
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> indexOptions = Parameter.restrictedStringParam(
            "index_options",
            false,
            m -> toType(m).indexOptions,
            "docs",
            "freqs"
        );
        private final Parameter<Boolean> indexLeafValuesIntoRoot = Parameter.boolParam(
            "index_leaf_values_into_root",
            false,
            m -> toType(m).indexLeafValuesIntoRoot,
            true
        );

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<ParametrizedFieldMapper.Parameter<?>> getParameters() {
            return Arrays.asList(
                indexed,
                hasDocValues,
                nullValue,
                indexOptions,
                indexLeafValuesIntoRoot,
                meta
            );
        }

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            fieldtype.setIndexOptions(TextParams.toIndexOptions(this.indexed.getValue(), this.indexOptions.getValue()));

            return new FlatObjectFieldMapper(
                name,
                new RootObjectFieldType(buildFullName(context), fieldtype, this),
                fieldtype,
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new FlatObjectFieldMapper.Builder(n));

    public static final class RootObjectFieldType extends StringFieldType {
        public RootObjectFieldType(
            String name,
            FieldType fieldType,
            Builder builder
        ) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                fieldType.stored(),
                builder.hasDocValues.getValue(),
                TextSearchInfo.SIMPLE_MATCH_ONLY,
                builder.meta.getValue()
            );
        }


        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            // keywords are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    public static final class ChildObjectFieldType extends StringFieldType {
        public ChildObjectFieldType(
            String name,
            FieldType fieldType,
            Builder builder
        ) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                fieldType.stored(),
                builder.hasDocValues.getValue(),
                TextSearchInfo.SIMPLE_MATCH_ONLY,
                builder.meta.getValue()
            );
        }



        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }


        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            // keywords are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }


        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean indexLeafValuesIntoRoot;
    private final String nullValue;
    private final String indexOptions;
    private final FieldType fieldType;
    private final Builder builder;
    private final FlatObjectFieldParser fieldParser;

    public FlatObjectFieldMapper(String simpleName,
                                 MappedFieldType defaultFieldType,
                                 FieldType fieldType,
                                 CopyTo copyTo,
                                 Builder builder) {
        super(simpleName, defaultFieldType, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.indexed = builder.indexed.getValue();
        this.indexOptions = builder.indexOptions.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.indexLeafValuesIntoRoot = builder.indexLeafValuesIntoRoot.getValue();
        this.fieldType = fieldType;
        this.builder = builder;
        this.fieldParser = new FlatObjectFieldParser(simpleName, indexed, hasDocValues, indexLeafValuesIntoRoot, nullValue);
    }

    @Override
    public RootObjectFieldType fieldType() {
        // this override is not really needed but it makes DynamoKeyFieldMapper more explicit in the mapper
        return (RootObjectFieldType) super.fieldType();
    }

    @Override
    public MappedFieldType keyedFieldType(String key) {
        return new ChildObjectFieldType(name(), fieldType, builder);
    }



    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
// context.path().pathAsText("aa") -> "test_flat_object.aa" when on root
        XContentParser xContentParser = context.parser();


        if (xContentParser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }


        if (mappedFieldType.isSearchable() == false && mappedFieldType.hasDocValues() == false) {
            xContentParser.skipChildren();
            return;
        }

        context.doc().addAll(fieldParser.parseFlatObject(xContentParser));

        if (mappedFieldType.hasDocValues() == false) {
            createFieldNamesField(context);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
