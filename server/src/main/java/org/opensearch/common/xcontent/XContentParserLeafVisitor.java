/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.ParsingException;
import org.opensearch.index.mapper.ContentPath;

import java.io.IOException;

/**
 * Helper abstract class which Iterate/Visit {@link XContentParser} recursively while maintaining {@link ContentPath} which tracks current
 * path in the traversal. It only handles {@link XContentParser.Token#START_ARRAY} and {@link XContentParser.Token#START_OBJECT} out of the
 * box.
 * Remaining {@link XContentParser.Token} will be delegated to
 * {@link XContentParserLeafVisitor#handleConcreteLeafValue(ContentPath, XContentParser)} which need implementation.
 */
public abstract class XContentParserLeafVisitor {
    protected final XContentParser parser;
    protected final ContentPath contentPath;

    /**
     * @param xContentParser current xContentParser, expecting {@link XContentParser.Token} to be at
     * {@link XContentParser.Token#START_OBJECT} or {@link XContentParser.Token#START_ARRAY}
     * @throws ParsingException if the parser isn't positioned at either START_OBJECT or START_ARRAY at the beginning
     */
    protected XContentParserLeafVisitor(XContentParser xContentParser) {
        validateCurrentPosition(xContentParser);
        this.parser = xContentParser;
        this.contentPath = new ContentPath();
    }

    /**
     * @throws ParsingException if the parser isn't positioned at either START_OBJECT or START_ARRAY at the beginning
     * @param xContentParser to validate
     */
    protected void validateCurrentPosition(XContentParser xContentParser) {
        XContentParser.Token token = xContentParser.currentToken();
        if (token != XContentParser.Token.START_ARRAY && token != XContentParser.Token.START_OBJECT) {
            XContentParserUtils.throwUnknownToken(token, xContentParser.getTokenLocation());
        }
    }

    /**
     * Entry point to start visiting all concrete leaf values.
     * @throws IOException if anything went wrong during parsing or if the type or name cannot be derived
     *                     from the field's name
     * @throws ParsingException if the parser isn't positioned at either START_OBJECT or START_ARRAY at the beginning
     */
    public void visitLeaves() throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            contentPath.add(parser.currentName());
            visitObject();
            contentPath.remove();
        } else if (token == XContentParser.Token.START_ARRAY) {
            visitList();
        } else {
            handleConcreteLeafValue(contentPath, parser);
        }
    }

    /**
     * XContent list iterator
     * @throws ParsingException if the parser isn't positioned START_ARRAY at the beginning
     */
    protected void visitList() throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        for (
            XContentParser.Token nextToken = parser.nextToken();
            nextToken != null && nextToken != XContentParser.Token.END_ARRAY;
            nextToken = parser.nextToken()
        ) {
            visitLeaves();
        }
    }

    /**
     * XContent object/map iterator
     * @throws ParsingException if the parser isn't positioned START_ARRAY at the beginning
     */
    protected void visitObject() throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        parser.nextToken();
        do {
            // parser must point to field name now
            // And then the value...
            parser.nextToken();
            visitLeaves();
        } while (parser.nextToken() == XContentParser.Token.FIELD_NAME);
    }

    /**
     * When visitor encounter a value which is not Array neither Object then this method will be called to delegate visiting the concrete
     * leaf field.
     * @param path current path from root up to this leaf not including current field name
     * @param parser positioned at the concrete value so to access field name refer to {@link XContentParser#currentName()}
     * @throws IOException if anything went wrong during parsing
     */
    protected abstract void handleConcreteLeafValue(ContentPath path, XContentParser parser) throws IOException;
}
