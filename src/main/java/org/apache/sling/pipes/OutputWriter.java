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

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.sling.pipes.CommandUtil.stringToMap;

/**
 * defines how pipe's output get written to a servlet response or output stream
 */
public abstract class OutputWriter {
    private static final Logger LOG = LoggerFactory.getLogger(OutputWriter.class);

    public static final String KEY_SIZE = "size";

    public static final String KEY_ITEMS = "items";

    public static final String KEY_ERRORS = "errors";

    public static final String KEY_NB_ERRORS = "nbErrors";

    protected static final String NEW_LINE = "\n";

    public static final String PARAM_SIZE = KEY_SIZE;

    public static final int NB_MAX = 10;

    protected boolean autoClose = true;

    protected long size;

    protected long nbErrors;

    protected long max = NB_MAX;

    protected Pipe pipe;

    protected Writer writer;

    public static final String PATH_KEY = "path";

    public static final String PARAM_WRITER = "writer";

    protected Map<String, Object> customOutputs;

    protected List<String> errors = new ArrayList<>();

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
        String writerParam = request.getParameter(PARAM_WRITER);
        if (StringUtils.isNotBlank(writerParam)){
            customOutputs = stringToMap(writerParam, PipeBindings::embedAsScript);
        }
        setWriter(response.getWriter());
        initResponse(response);
        starts();
    }

    /**
     * @param customOutputs custom outputs
     */
    public void setCustomOutputs(Map<String, Object> customOutputs) {
        this.customOutputs = customOutputs;
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

    /**x
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
     * Write a given error
     * @param path resource that lead to the error
     */
    public void error(String path) {
        if (nbErrors++ < max) {
            errors.add(path);
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
     * @return true if to be closed internally by the plumber
     */
    public boolean autoClose() {
        return autoClose;
    }

    public void disableAutoClose() {
        autoClose = false;
    }

    /**
     * Setter
     * @param pipe pipe this writer should be associated with
     */
    public void setPipe(Pipe pipe) {
        this.pipe = pipe;
        Resource outputs = pipe.getResource().getChild(PARAM_WRITER);
        if (customOutputs == null && outputs != null ){
            customOutputs = new HashMap<>();
            customOutputs.putAll(outputs.getValueMap());
            for (String ignoredKey : BasePipe.IGNORED_PROPERTIES) {
                customOutputs.remove(ignoredKey);
            }
        }
    }

    protected String computeValue(String key) {
        String value = EMPTY;
        try {
            Object o = pipe.getBindings().instantiateObject((String) customOutputs.get(key));
            if (o != null) {
                if (o instanceof Calendar) {
                    Instant i = ((Calendar)o).toInstant();
                    value = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC)).format(i);
                } else {
                    value = o.toString();
                }
            }
        } catch (RuntimeException e) {
            LOG.debug("unable to write entry {}, will write empty value", key, e);
        }
        return value;
    }

    @Override
    public String toString() {
        return writer.toString();
    }

    public Map<String, Object> getCustomOutputs() {
        return customOutputs;
    }
}
