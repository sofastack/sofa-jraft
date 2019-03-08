/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rhea.util;

import java.util.List;

/**
 * Static utility methods pertaining to {@code String} or {@code CharSequence}
 * instances.
 *
 * @author jiachun.fjc
 */
public final class Strings {

    /**
     * Returns the given string if it is non-null; the empty string otherwise.
     */
    public static String nullToEmpty(final String string) {
        return (string == null) ? "" : string;
    }

    /**
     * Returns the given string if it is nonempty; {@code null} otherwise.
     */
    public static String emptyToNull(final String string) {
        return isNullOrEmpty(string) ? null : string;
    }

    /**
     * Returns {@code true} if the given string is null or is the empty string.
     */
    public static boolean isNullOrEmpty(final String str) {
        return str == null || str.length() == 0;
    }

    /**
     * Checks if a string is whitespace, empty ("") or null.
     *
     * Strings.isBlank(null)      = true
     * Strings.isBlank("")        = true
     * Strings.isBlank(" ")       = true
     * Strings.isBlank("bob")     = false
     * Strings.isBlank("  bob  ") = false
     */
    public static boolean isBlank(final String str) {
        final int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            for (int i = 0; i < strLen; i++) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Checks if a string is not empty (""), not null and not whitespace only.
     *
     * Strings.isNotBlank(null)      = false
     * Strings.isNotBlank("")        = false
     * Strings.isNotBlank(" ")       = false
     * Strings.isNotBlank("bob")     = true
     * Strings.isNotBlank("  bob  ") = true
     */
    public static boolean isNotBlank(final String str) {
        return !isBlank(str);
    }

    /**
     * Splits the provided text into an array, separator specified.
     *
     * A null input String returns null.
     *
     * Strings.split(null, *)         = null
     * Strings.split("", *)           = []
     * Strings.split("a.b.c", '.')    = ["a", "b", "c"]
     * Strings.split("a..b.c", '.')   = ["a", "b", "c"]
     * Strings.split("a:b:c", '.')    = ["a:b:c"]
     * Strings.split("a b c", ' ')    = ["a", "b", "c"]
     */
    public static String[] split(final String str, final char separator) {
        return split(str, separator, false);
    }

    /**
     * Splits the provided text into an array, separator specified,
     * if {@code} true, preserving all tokens, including empty tokens created
     * by adjacent separators.
     *
     * A null input String returns null.
     *
     * Strings.split(null, *, true)         = null
     * Strings.split("", *, true)           = []
     * Strings.split("a.b.c", '.', true)    = ["a", "b", "c"]
     * Strings.split("a..b.c", '.', true)   = ["a", "", "b", "c"]
     * Strings.split("a:b:c", '.', true)    = ["a:b:c"]
     * Strings.split("a b c", ' ', true)    = ["a", "b", "c"]
     * Strings.split("a b c ", ' ', true)   = ["a", "b", "c", ""]
     * Strings.split("a b c  ", ' ', true)  = ["a", "b", "c", "", ""]
     * Strings.split(" a b c", ' ', true)   = ["", a", "b", "c"]
     * Strings.split("  a b c", ' ', true)  = ["", "", a", "b", "c"]
     * Strings.split(" a b c ", ' ', true)  = ["", a", "b", "c", ""]
     */
    public static String[] split(final String str, final char separator, final boolean preserveAllTokens) {
        if (str == null) {
            return null;
        }
        final int len = str.length();
        if (len == 0) {
            return EMPTY_STRING_ARRAY;
        }
        final List<String> list = Lists.newArrayList();
        int i = 0, start = 0;
        boolean match = false;
        while (i < len) {
            if (str.charAt(i) == separator) {
                if (match || preserveAllTokens) {
                    list.add(str.substring(start, i));
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match || preserveAllTokens) {
            list.add(str.substring(start, i));
        }
        return list.toArray(new String[0]);
    }

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private Strings() {
    }
}
