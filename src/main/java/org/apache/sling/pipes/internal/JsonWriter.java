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

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.OutputWriter;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import java.io.StringWriter;
import java.util.Map;

/**
 * default output writer, that outputs JSON with size and output resources' path
 */
public class JsonWriter extends OutputWriter {
    protected JsonGenerator jsonGenerator;

    public static final String JSON_EXTENSION = "json";

    JsonWriter(){
        setWriter(new StringWriter());
    }

    @Override
    public boolean handleRequest(SlingHttpServletRequest request) {
        return JSON_EXTENSION.equals(request.getRequestPathInfo().getExtension());
    }

    @Override
    protected void initResponse(SlingHttpServletResponse response){
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");
    }

    @Override
    public void starts() {
        jsonGenerator = Json.createGenerator(writer);
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStartArray(KEY_ITEMS);
    }

    @Override
    public void writeItem(Resource resource) {
        if (customOutputs == null) {
            jsonGenerator.write(resource.getPath());
        } else {
            jsonGenerator.writeStartObject();
            jsonGenerator.write(PATH_KEY, resource.getPath());
            for (Map.Entry<String, Object> entry : customOutputs.entrySet()) {
                jsonGenerator.write(entry.getKey(), computeValue(entry.getKey()));
            }
            jsonGenerator.writeEnd();
        }
    }

    @Override
    public void ends() {
        jsonGenerator.writeEnd();
        jsonGenerator.write(KEY_SIZE,size);
        if (nbErrors > 0) {
            jsonGenerator.write(KEY_NB_ERRORS, nbErrors);
            jsonGenerator.writeStartArray(KEY_ERRORS);
            for (String error : errors){
                jsonGenerator.write(error);
            }
            jsonGenerator.writeEnd();
        }
        jsonGenerator.writeEnd();
        jsonGenerator.flush();
    }
}