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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Input Stream based pipe, coming from web, from request, resource tree, web
 * binding is updated by the returned iterator
 */
public abstract class AbstractInputStreamPipe extends BasePipe {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInputStreamPipe.class);

    public static final String REMOTE_START = "http";

    protected static final Pattern VALID_PATH = Pattern.compile("/([\\w\\.\\-_]+/)+[\\w\\.\\-_]+");

    public static final String BINDING_IS = "org.apache.sling.pipes.RequestInputStream";

    private static final String BASIC_AUTH_BINDINGS = "basicAuth";

    private static final String AUTH_HEADER = "Authentication";

    private static final String BASIC_PREFIX = "Basic ";
    private static final String PN_URL_MODE = "url_mode";
    private static final String URL_MODE_FETCH = "FETCH";
    private static final String URL_MODE_AS_IS = "AS_IS";

    protected Object binding;

    InputStream is;

    public AbstractInputStreamPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        binding = null;
    }

    InputStream getInputStreamFromResource(String expr) {
        Resource resource = resolver.getResource(expr);
        if (resource != null) {
            return resource.adaptTo(InputStream.class);
        }
        return null;
    }

    InputStream getInputStream() throws IOException {
        String expr = getExpr();
        if (expr.startsWith(REMOTE_START) && !properties.get(PN_URL_MODE, URL_MODE_FETCH).equalsIgnoreCase(URL_MODE_AS_IS)) {
            //first look at
            String urlExpression = getExpr();
            if (StringUtils.isNotBlank(urlExpression)) {
                URL url = new URL(urlExpression);
                URLConnection urlConnection = url.openConnection();
                String basicAuth = (String)getBindings().getBindings().get(BASIC_AUTH_BINDINGS);
                if (StringUtils.isNotBlank(basicAuth)) {
                    LOGGER.debug("Configuring basic authentication for {}", urlConnection);
                    HttpURLConnection connection = (HttpURLConnection) urlConnection;
                    String encoded = Base64.getEncoder().encodeToString(basicAuth.getBytes(StandardCharsets.UTF_8));
                    connection.setRequestProperty(AUTH_HEADER, BASIC_PREFIX + encoded);
                }
                LOGGER.debug("Executing GET {}", url);
                return urlConnection.getInputStream();
            }
        }
        if (VALID_PATH.matcher(expr).find()) {
            InputStream resourceIs = getInputStreamFromResource(expr);
            if (resourceIs != null) {
                return resourceIs;
            }
        }
        if (getBindings().getBindings().get(BINDING_IS) != null) {
            return (InputStream)getBindings().getBindings().get(BINDING_IS);
        }
        return new ByteArrayInputStream(expr.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Object getOutputBinding() {
        return binding;
    }

    public abstract Iterator<Resource> getOutput(InputStream inputStream);

    @Override
    public Iterator<Resource> computeOutput() {
        try {
            is = getInputStream();
            return getOutput(is);
        }  catch (IOException e) {
            throw new IllegalArgumentException(e);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
}
