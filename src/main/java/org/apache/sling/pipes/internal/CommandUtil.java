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

import java.util.Map;

/**
 * utilities for user input
 */
public class CommandUtil {

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
     * write key/value pairs into a map
     * @param map target map
     * @param params key/value pairs to write into the map
     */
    public static void writeToMap(Map<String, Object> map, Object... params){
        for (int i = 0; i < params.length - 1; i += 2){
            map.put(params[i].toString(), params[i + 1]);
        }
    }

}
