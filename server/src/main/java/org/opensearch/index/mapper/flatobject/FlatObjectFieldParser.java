/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FlatObjectFieldParser {
    public static final String FLAT_FIELD_OBJECT_CONTENT_KEY_SUFFIX = "-content";

    private final String rootFieldName;
    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean indexLeafValuesIntoRoot;
    private final String nullValue;

    public FlatObjectFieldParser(
        String rootFieldName,
        boolean indexed,
        boolean hasDocValues,
        boolean indexLeafValuesIntoRoot,
        String nullValue
    ) {
        this.rootFieldName = rootFieldName;
        this.indexed = indexed;
        this.hasDocValues = hasDocValues;
        this.indexLeafValuesIntoRoot = indexLeafValuesIntoRoot;
        this.nullValue = nullValue;
    }

    List<IndexableField> parseFlatObject(final XContentParser xContentParser) throws IOException {
        List<IndexableField> expandedFields = new ArrayList<>();
        XContentParserUtils.visitConcreteLeaves(xContentParser, (path, parser) -> {
            String leafFieldName = parser.currentName();
            String rawValue;
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                rawValue = nullValue;
            } else {
                rawValue = parser.text();
            }

            String absoluteKey = path.pathAsText(leafFieldName);
            FlatObjectFieldUtils.validateKeyUsable(absoluteKey);
            String keyValuePair = FlatObjectFieldUtils.createKeyValuePair(absoluteKey, rawValue);
            String rootContentFieldName = createRootContentFieldName();
            BytesRef valueBytes = new BytesRef(rawValue);
            BytesRef keyValuePairBytes = new BytesRef(keyValuePair);

            if (indexed) {
                if (indexLeafValuesIntoRoot) {
                    expandedFields.add(new StringField(rootFieldName, valueBytes, Field.Store.NO));
                }
                expandedFields.add(new StringField(rootContentFieldName, keyValuePairBytes, Field.Store.NO));
            }

            if (hasDocValues) {
                if (indexLeafValuesIntoRoot) {
                    expandedFields.add(new SortedSetDocValuesField(rootFieldName, valueBytes));
                }
                expandedFields.add(new SortedSetDocValuesField(rootContentFieldName, keyValuePairBytes));
            }
        });
        return expandedFields;
    }

    private String createRootContentFieldName() {
        return rootFieldName + FLAT_FIELD_OBJECT_CONTENT_KEY_SUFFIX;
    }

}
