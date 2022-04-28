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
package org.apache.sling.pipes.internal.inputstream;

import org.apache.commons.collections4.keyvalue.DefaultKeyValue;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.AbstractInputStreamPipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.internal.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Pipe outputting binding related to a json stream: either an object
 */
public class JsonPipe extends AbstractInputStreamPipe {
    private static Logger logger = LoggerFactory.getLogger(JsonPipe.class);
    public static final String JSON_KEY = "json";
    public static final String RESOURCE_TYPE = RT_PREFIX + JSON_KEY;

    /**
     * property specifying the json path where to fetched the used value
     */
    protected static final String PN_VALUEPATH = "valuePath";

    /**
     * property specifying wether we should bind the computed json as a whole, or loop over it (not set or raw=false)
     */
    protected static final String PN_RAW = "raw";

    protected static final String JSONPATH_ROOT = "$";

    protected static final String ARRAY_START = "[";

    protected static final String OBJ_START = ".";

    protected static final String INDEX_SUFFIX = "_index";

    protected static final Pattern JSONPATH_FIRSTTOKEN = Pattern.compile("^\\" + JSONPATH_ROOT + "([\\" + OBJ_START + "\\" + ARRAY_START + "])([^\\" + OBJ_START + "\\]\\" + ARRAY_START + "]+)\\]?");

    JsonBindingIterator internalIterator;

    public JsonPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
    }

    boolean isRaw() {
        if (properties.containsKey(PN_RAW)) {
            Object raw = bindings.instantiateObject(properties.get(PN_RAW, String.class));
            if (raw instanceof Boolean) {
                return (Boolean) raw;
            }
            return Boolean.parseBoolean((String)raw);
        }
        return false;
    }

    /**
     * in case there is no successful retrieval of some JSON data, we cut the pipe here
     * @return input resource of the pipe, can be reouputed N times in case output json binding is an array of
     * N element (output binding would be here each time the Nth element of the array)
     */
    @Override
    public Iterator<Resource> getOutput(InputStream is) {
        Iterator<Resource> output = EMPTY_ITERATOR;
        Iterator<Resource> inputSingletonIterator = Collections.singleton(getInput()).iterator();
        try {
            String jsonString = IOUtils.toString(is, StandardCharsets.UTF_8);
            if (StringUtils.isNotBlank(jsonString)) {
                JsonStructure json;
                json = JsonUtil.parse(jsonString);
                if (json == null) {
                    binding = jsonString.trim();
                    output = inputSingletonIterator;
                } else {
                    String valuePath = properties.get(PN_VALUEPATH, String.class);
                    if (StringUtils.isNotBlank(valuePath)) {
                        json = getValue(json, bindings.instantiateExpression(valuePath));
                    }
                    if (isRaw() || !(json.getValueType() == ValueType.ARRAY || json.getValueType() == ValueType.OBJECT)) {
                        binding = JsonUtil.unbox(json);
                        output = inputSingletonIterator;
                    } else {
                        binding = json;
                        output = internalIterator = new JsonBindingIterator(json, getInput());
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
        return output;
    }

    class JsonBindingIterator implements Iterator<Resource> {
        Collection<Object> jsonValues;
        Iterator<Object> internal;
        final Resource inputResource;
        int index = 0;

        JsonBindingIterator(JsonStructure json, Resource inputResource) {
            jsonValues = json.getValueType() == ValueType.ARRAY ?
                    ((JsonArray) json).stream().map(JsonUtil::unbox).collect(Collectors.toList()):
                    ((JsonObject) json).entrySet().stream()
                            .map(e -> new DefaultKeyValue<String,Collection<Object>>(e.getKey(),
                                    JsonUtil.unbox(e.getValue()))).collect(Collectors.toList());
            internal = jsonValues.iterator();
            this.inputResource = inputResource;
        }

        @Override
        public boolean hasNext() {
            return internal.hasNext();
        }

        @Override
        public Resource next() {
            if (! internal.hasNext()) {
                throw new NoSuchElementException();
            }
            binding = internal.next();
            getBindings().addBinding(getName() + INDEX_SUFFIX, index ++);
            return inputResource;
        }
    }

    /**
     * Returns fetched json value from value path
     * @param json json structure from which to start
     * @param valuePath path to follow
     * @return value fetched after following the path
     */
    protected JsonStructure getValue(JsonStructure json, String valuePath){
        Matcher matcher = JSONPATH_FIRSTTOKEN.matcher(valuePath);
        if (matcher.find()){
            String firstChar = matcher.group(1);
            String content = matcher.group(2);
            logger.trace("first char is {}, content is {}", firstChar, content);
            if (ARRAY_START.equals(firstChar)){
                JsonArray array = (JsonArray)json;
                int index = Integer.parseInt(content);
                json = (JsonStructure)array.get(index);
            } else if (OBJ_START.equals(firstChar)){
                JsonObject object = (JsonObject)json;
                json = (JsonStructure)object.get(content);
            }
            valuePath = StringUtils.removeStart(valuePath, matcher.group(0));
            if (StringUtils.isNotBlank(valuePath)){
                valuePath = JSONPATH_ROOT + valuePath;
                return getValue(json, valuePath);
            }
        }
        return json;
    }

}
