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

import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import javax.json.JsonValue;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.Pipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * writes current resource, dubbing a eventual parameter or given child resource "writer" property/value pairs, allowing
 * expressions
 */
public class CustomJsonWriter extends DefaultJsonWriter {
    Logger log = LoggerFactory.getLogger(CustomJsonWriter.class);

    public static final String PATH_KEY = "path";

    public static final String PARAM_WRITER = "writer";

    Map<String, Object> customOutputs;

    CustomJsonWriter() {
    }

    CustomJsonWriter(Writer writer) {
        super(writer);
    }

    @Override
    public boolean handleRequest(SlingHttpServletRequest request) {
        String writerParam = request.getParameter(PARAM_WRITER);
        if (StringUtils.isNotBlank(writerParam)){
            try {
                customOutputs = JsonUtil.unbox(JsonUtil.parseObject(writerParam));
                return true;
            } catch(Exception e){
                log.error("requested json writer can't be parsed", e);
            }
        } else {
            Resource resource = request.getResource().getChild(PARAM_WRITER);
            return resource != null;
        }
        return false;
    }

    @Override
    public void setPipe(Pipe pipe) {
        super.setPipe(pipe);
        if (customOutputs == null){
            customOutputs = new HashMap<>();
            customOutputs.putAll(pipe.getResource().getChild(PARAM_WRITER).adaptTo(ValueMap.class));
            for (String ignoredKey : BasePipe.IGNORED_PROPERTIES) {
                customOutputs.remove(ignoredKey);
            }
        }
    }

    @Override
    public void writeItem(Resource resource) {
        jsonWriter.writeStartObject();
        jsonWriter.write(PATH_KEY,resource.getPath());
        for (Map.Entry<String, Object> entry : customOutputs.entrySet()){
            Object o = pipe.getBindings().instantiateObject((String)entry.getValue());
            if ( o instanceof JsonValue ) {
                jsonWriter.write(entry.getKey(),(JsonValue) o);
            }
            else {
                jsonWriter.write(entry.getKey(), o.toString());
            }
        }
        jsonWriter.writeEnd();
    }
}