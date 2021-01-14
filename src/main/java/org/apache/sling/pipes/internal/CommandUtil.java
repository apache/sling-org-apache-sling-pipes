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
package org.apache.sling.pipes.internal;

import org.apache.sling.pipes.PipeBindings;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.sling.pipes.PipeBindings.INJECTED_SCRIPT_REGEXP;

/**
 * utilities for user input
 */
public class CommandUtil {
    static final String PAIR_SEP = ",";
    static final String KEY_VALUE_SEP = "=";
    static final String FIRST_TOKEN = "first";
    static final String SECOND_TOKEN = "second";
    private static final Pattern UNEMBEDDEDSCRIPT_PATTERN = Pattern.compile("^(\\d+(\\.\\d+)?)|" + //21.42
            "(\\[.*]$)|" + //['one','two']
            "(\\w[\\w_\\-\\d]+\\..+)|" + //map.field...
            "(\\w[\\w_\\-\\d]+\\['.+'])|" + //map['field']
            "(true$|false$)|" + //boolean
            "(new .*)|" + //instantiation
            "(.*'$)"); // 'string'
    static final String CONFIGURATION_TOKEN = "(?<" + FIRST_TOKEN + ">[\\w/\\:]+)\\s*" + KEY_VALUE_SEP
            + "(?<" + SECOND_TOKEN + ">[(\\w*)|" + INJECTED_SCRIPT_REGEXP + "]+)";
    public static final Pattern CONFIGURATION_PATTERN = Pattern.compile(CONFIGURATION_TOKEN);

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
        }
    }

    /**
     * @param input comma separated key=value pairs
     * @param valueTransformer before adding it to the map, that function will be applied to found value
     * @return map of key and (transformed) value
     */
    public static Map stringToMap(@NotNull String input, Function<String, String> valueTransformer) {
        Map<String, Object> map = new HashMap<>();
        for (String pair : input.split(PAIR_SEP) ){
            Matcher matcher = CONFIGURATION_PATTERN.matcher(pair);
            if (matcher.find()) {
                map.put(matcher.group(FIRST_TOKEN), valueTransformer.apply(matcher.group(SECOND_TOKEN)));
            }
        }
        return map;
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
                    args.add(matcher.group(FIRST_TOKEN));
                    args.add(matcher.group(SECOND_TOKEN));
                }
            }
        }
        return args.toArray(new String[args.size()]);
    }

}
