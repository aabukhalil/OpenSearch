/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.flatobject;

import java.nio.charset.StandardCharsets;

public final class FlatObjectFieldUtils {
    // bell character in ASCII (https://www.lookuptables.com/text/ascii-table), probably the most rare to encounter as input
    public static final byte KEY_VALUE_SEPARATOR_BYTE = '\u0007';
    public static final String KEY_VALUE_SEPARATOR = "\u0007";

    public static String createKeyValuePair(String key, String value) {
        return key + KEY_VALUE_SEPARATOR + value;
    }

    public static void validateKeyUsable(String key) {
        if (key.contains(KEY_VALUE_SEPARATOR)) {
            throw new IllegalArgumentException("[flat] field keys cannot contain reserved character \\u0007");
        }
    }



}
