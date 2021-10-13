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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CsvWriter extends OutputWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvWriter.class);

    private static final String CSV_EXTENSION = "csv";

    private static final String SEPARATOR = ",";

    private static final String HEADER_ERROR = "errors";

    List<String> headers;

    @Override
    public boolean handleRequest(SlingHttpServletRequest request) {
        return CSV_EXTENSION.equals(request.getRequestPathInfo().getExtension());
    }

    @Override
    protected void initResponse(SlingHttpServletResponse response) {
        response.setCharacterEncoding("utf-8");
        response.setContentType("plain/text");
    }

    @Override
    public void starts() {
        //nothing to start with
    }

    void writeCell(Resource resource, List<String> elts, String key) {
        if (key.equals(PATH_KEY)){
            elts.add(resource.getPath());
        } else if (customOutputs != null) {
            elts.add(computeValue(key));
        }
    }

    @Override
    protected void writeItem(Resource resource) {
        if (headers == null) {
            headers = new ArrayList<>();
            headers.add(PATH_KEY);
            if (customOutputs != null) {
                headers.addAll(customOutputs.keySet());
            }
            try {
                writer.write(headers.stream().collect(Collectors.joining(SEPARATOR)) + NEW_LINE);
            } catch (IOException e) {
                LOGGER.error("unable to write header");
            }
        }
        try {
            List<String> elts = new ArrayList<>();
            for (String key : headers){
                writeCell(resource, elts, key);
            }
            String line = elts.stream().collect(Collectors.joining(SEPARATOR));
            writer.write(line + NEW_LINE);
        } catch (IOException e) {
            LOGGER.error("unable to write header", e);
        }
    }

    @Override
    public void ends() {
        try {
            if (!errors.isEmpty()){
                writer.write(HEADER_ERROR + NEW_LINE);
                for (String error : errors){
                    writer.write(error + NEW_LINE);
                }
            }
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("unable to flush", e);
        }
    }
}