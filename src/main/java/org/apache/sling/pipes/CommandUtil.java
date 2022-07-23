/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sling.pipes;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.sling.pipes.PipeBindings.INJECTED_SCRIPT_REGEXP;

/**
 * utilities for user input
 */
public class CommandUtil {
    static final String PAIR_SEP = ",";
    static final String KEY_VALUE_SEP = "=";
    static final String FIRST_KEY = "first";
    static final String SECOND_KEY = "second";
    static final String PN_JCR_MIXIN = "jcr:mixinTypes";

    static final String QUOTE = "\"";
    static final Pattern MIXINS_ARRAY_PATTERN = Pattern.compile("^\\s*\\[(.*)\\]\\s*$");
    private static final Pattern UNEMBEDDEDSCRIPT_PATTERN = Pattern.compile("^(\\d+(\\.\\d+)?)|" + //21.42
            "\\[.*]$|" + //['one','two']
            "[\\w_\\-]+\\..+|" + //map.field...
            "[\\w_\\-]+\\['.+']|" + //map['field']
            "true$|false$|" + //boolean
            "new .*|" + //instantiation
            "(.*'$)"); // 'string'
    static final String EXPR_TOKEN = "([^=]+|" + INJECTED_SCRIPT_REGEXP + ")+";
    static final String CONFIGURATION_TOKEN = "\\s*(?<" + FIRST_KEY + ">" + EXPR_TOKEN + ")\\s*" + KEY_VALUE_SEP
            + "\\s*(?<" + SECOND_KEY + ">(" + EXPR_TOKEN + ")+)\\s*";
    public static final Pattern CONFIGURATION_PATTERN = Pattern.compile(CONFIGURATION_TOKEN);

    private static final String ESCAPED_EQ = "_EQ_";
    private CommandUtil() {
    }

    /**
     * Checks arguments and throws exception if there is an issue
     * @param params arguments to check
     * @throws IllegalArgumentException exception thrown in case arguments are wrong
     */
    public static void checkArguments(Object... params) {
        if (params.length % 2 > 0){
            throw new IllegalArgumentException("there should be an even number of arguments");
        }
    }

    /**
     * @param value
     * @return eventually wrapped value
     */
    static Object embedIfNeeded(Object value) {
        if (value instanceof String) {
            Matcher matcher = UNEMBEDDEDSCRIPT_PATTERN.matcher(value.toString());
            if (matcher.matches()) {
                return PipeBindings.embedAsScript(value.toString());
            }
        }
        return value;
    }

    /**
     * write key/value pairs into a map
     * @param map target map
     * @param embed flag indicating wether or not we should try to embed values in script tags,
     * @param params key/value pairs to write into the map
     */
    public static void writeToMap(Map<String, Object> map, boolean embed, Object... params){
        for (int i = 0; i < params.length - 1; i += 2) {
            map.put(params[i].toString(), embed ? embedIfNeeded(params[i + 1]) : params[i + 1]);
            if (params[i].toString().equals(PN_JCR_MIXIN)) {
                map.put(PN_JCR_MIXIN, handleMixins((String)params[i + 1]));
            }
        }
    }

    static String[] handleMixins(String value) {
        Matcher matcher = MIXINS_ARRAY_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Arrays.stream(matcher.group(1).split(PAIR_SEP))
                    .map(String::trim)
                    .collect(Collectors.toList()).toArray(new String[0]);
        }
        return new String[] { value };
    }

    /**
     * @param input comma separated key=value pairs
     * @param valueTransformer before adding it to the map, that function will be applied to found value
     * @return map of key and (transformed) value
     */
    public static Map<String, Object> stringToMap(@NotNull String input, UnaryOperator<String> valueTransformer) {
        Map<String, Object> map = new HashMap<>();
        for (String pair : input.split(PAIR_SEP) ){
            Matcher matcher = CONFIGURATION_PATTERN.matcher(pair);
            if (matcher.find()) {
                map.put(matcher.group(FIRST_KEY), valueTransformer.apply(matcher.group(SECOND_KEY)));
            }
        }
        return map;
    }

    /**
     * @param quotedString non null string with or without quotes
     * @return if the string is wrapped with <code>"</code>, strip them away, otherwise return same string
     */
    public static String trimQuotes(@NotNull String quotedString) {
        if (quotedString.startsWith(QUOTE) && quotedString.endsWith(QUOTE)) {
            return quotedString.substring(1, quotedString.length() - 1);
        }
        return quotedString;
    }

    /**
     * @param o list of key value strings key1=value1,key2=value2,...
     * @return String [] key1,value1,key2,value2,... corresponding to the pipe builder API
     */
    public static String[] keyValuesToArray(List<String> o) {
        List<String> args = new ArrayList<>();
        if (o != null) {
            for (String pair : o) {
                Matcher matcher = CONFIGURATION_PATTERN.matcher(pair.trim());
                if (matcher.matches()) {
                    args.add(trimQuotes(matcher.group(FIRST_KEY)));
                    args.add(trimQuotes(matcher.group(SECOND_KEY)).replaceAll(ESCAPED_EQ, "="));
                }
            }
        }
        return args.toArray(new String[args.size()]);
    }

}
