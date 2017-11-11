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

import java.io.IOException;
import java.io.Writer;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.Resource;

/**
 * defines how pipe's output get written to a servlet response or output stream
 */
public abstract class OutputWriter {

    public static final String KEY_SIZE = "size";

    public static final String KEY_ITEMS = "items";

    public static final String PARAM_SIZE = KEY_SIZE;

    public static final int NB_MAX = 10;

    protected long size;

    protected long max = NB_MAX;

    protected Pipe pipe;

    protected Writer writer;

    /**
     *
     * @param request current request
     * @return true if this writer handles that request
     */
    public abstract boolean handleRequest(SlingHttpServletRequest request);

    /**
     * Init the writer, writes beginning of the output
     * @param request request from which writer will output
     * @param response response on which writer will output
     * @throws IOException error handling streams
     */
    public void init(SlingHttpServletRequest request, SlingHttpServletResponse response) throws IOException {
        if (request.getParameter(PARAM_SIZE) != null) {
            setMax(Integer.parseInt(request.getParameter(PARAM_SIZE)));
        }
        setWriter(response.getWriter());
        initResponse(response);
        starts();
    }

    /**
     * Specifically init the response
     * @param response response on which to write
     */
    protected abstract void initResponse(SlingHttpServletResponse response);

    /**
     * Init the writer, writes beginning of the output
     */
    public abstract void starts();

    /**
     * Setter for max (will put to max if value is negative)
     * @param max positive max value to set
     */
    public void setMax(int max) {
        this.max = max;
        if (max < 0) {
            this.max = Integer.MAX_VALUE;
        }
    }

    /**
     * Set the writer
     * @param writer writer on which to write output
     */
    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    /**
     * Write a given resource
     * @param resource resource that will be written
     */
    public void write(Resource resource) {
        if (size++ < max) {
            writeItem(resource);
        }
    }

    /**
     * Write a given resource
     * @param resource resource that will be written
     */
    protected abstract void writeItem(Resource resource);

    /**
     * writes the end of the output
     */

    public abstract void ends();

    /**
     * Setter
     * @param pipe pipe this writer should be associated with
     */
    public void setPipe(Pipe pipe) {
        this.pipe = pipe;
    }

    @Override
    public String toString() {
        return writer.toString();
    }
}
