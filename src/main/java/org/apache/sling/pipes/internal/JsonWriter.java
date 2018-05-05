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

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;
import javax.script.ScriptException;
import java.io.StringWriter;
import java.util.Map;

/**
 * default output writer, that outputs JSON with size and output resources' path
 */
public class JsonWriter extends OutputWriter {
    private static Logger logger = LoggerFactory.getLogger(JsonWriter.class);

    protected JsonGenerator jsonWriter;

    public static final String JSON_EXTENSION = "json";

    JsonWriter(){
        setWriter(new StringWriter());
    }

    @Override
    public boolean handleRequest(SlingHttpServletRequest request) {
        return request.getRequestPathInfo().getExtension().equals(JSON_EXTENSION);
    }

    @Override
    protected void initResponse(SlingHttpServletResponse response){
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");
    }

    @Override
    public void starts() {
        jsonWriter = Json.createGenerator(writer);
        jsonWriter.writeStartObject();
        jsonWriter.writeStartArray(KEY_ITEMS);
    }

    @Override
    public void writeItem(Resource resource) {
        if (customOutputs == null) {
            jsonWriter.write(resource.getPath());
        } else {
            jsonWriter.writeStartObject();
            jsonWriter.write(PATH_KEY, resource.getPath());
            for (Map.Entry<String, Object> entry : customOutputs.entrySet()) {
                Object o = null;
                try {
                    o = pipe.getBindings().instantiateObject((String) entry.getValue());
                    if (o instanceof JsonValue) {
                        jsonWriter.write(entry.getKey(), (JsonValue) o);
                    } else {
                        jsonWriter.write(entry.getKey(), o.toString());
                    }
                } catch (ScriptException e) {
                    logger.error("unable to write entry {}, will write empty value", entry, e);
                    jsonWriter.write(StringUtils.EMPTY);
                }
            }
            jsonWriter.writeEnd();
        }
    }

    @Override
    public void ends() {
        jsonWriter.writeEnd();
        jsonWriter.write(KEY_SIZE,size);
        jsonWriter.writeEnd();
        jsonWriter.flush();
    }
}