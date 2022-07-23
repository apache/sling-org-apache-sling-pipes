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

import static org.apache.commons.lang3.StringUtils.EMPTY;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private static final String HEADER_PREFIX = "header_";
    private static final String AUTH_HEADER = "Authentication";
    private static final String BASIC_PREFIX = "Basic ";
    private static final String PN_URL_MODE = "url_mode";
    private static final String URL_MODE_FETCH = "FETCH";
    private static final String URL_MODE_AS_IS = "AS_IS";

    private static final String METHOD_POST = "POST";

    private static final String HEADER_TYPE ="Content-Type";

    private static final String HEADER_LENGTH ="Content-Length";

    private static final String QUERY_CHAR = "?";

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

    void addHeaders(URLConnection connection) {
        connection.setRequestProperty( "charset", "utf-8");
        Collection<String> headers = properties.keySet().stream()
                .filter(k -> k.startsWith(HEADER_PREFIX))
                .map(s -> StringUtils.substringAfter(s, HEADER_PREFIX))
                .collect(Collectors.toList());
        if (!headers.isEmpty()) {
            for (String k : headers) {
                String value = getBindings().instantiateExpression(properties.get(HEADER_PREFIX + k, String.class));
                connection.setRequestProperty(k, value);
            }
            String basicAuth = (String)getBindings().getBindings().get(BASIC_AUTH_BINDINGS);
            if (StringUtils.isNotBlank(basicAuth)) {
                if (headers.contains(AUTH_HEADER)) {
                    LOGGER.warn("both authentication header & basic auth are set, ignoring basic auth");
                } else {
                    String encoded = Base64.getEncoder().encodeToString(basicAuth.getBytes(StandardCharsets.UTF_8));
                    connection.setRequestProperty(AUTH_HEADER, BASIC_PREFIX + encoded);
                }
            }
        }
    }

    private URLConnection preparePost(String expr) throws IOException {
        HttpURLConnection connection;
        String url = expr;
        String data = EMPTY;
        byte[] postData;
        if (expr.contains(QUERY_CHAR)) {
            data = StringUtils.substringAfter(expr, QUERY_CHAR);
            url = StringUtils.substringBefore(expr, QUERY_CHAR);
        } else {
            //we consider POST property value to be the request body
            data = getBindings().instantiateExpression(properties.get(METHOD_POST, String.class));
        }
        connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod(METHOD_POST);
        postData = data.getBytes(StandardCharsets.UTF_8);
        int postDataLength = postData.length;
        if (expr.contains(QUERY_CHAR)) {
            connection.setRequestProperty(HEADER_TYPE, "application/x-www-form-urlencoded");
        }
        connection.setRequestProperty( HEADER_LENGTH, Integer.toString( postDataLength ));
        connection.setDoOutput( true );
        connection.setInstanceFollowRedirects( false );
        addHeaders(connection);
        try( DataOutputStream wr = new DataOutputStream( connection.getOutputStream())) {
            wr.write( postData );
        }
        return connection;
    }

    URLConnection getConnection(String expr) throws IOException {
        boolean usePost = properties.containsKey(METHOD_POST);
        LOGGER.debug("Accessing {} (POST={})", expr, usePost);
        URLConnection urlConnection;
        if (usePost) {
            if (isDryRun()) {
                LOGGER.debug("we won't execute a POST request in a dry run");
                return null;
            }
            urlConnection = preparePost(expr);
        } else {
            URL url = new URL(expr);
            urlConnection = url.openConnection();
            addHeaders(urlConnection);
        }
        return urlConnection;
    }

    InputStream getInputStream() throws IOException {
        String expr = getExpr();
        if (expr.startsWith(REMOTE_START) && !properties.get(PN_URL_MODE, URL_MODE_FETCH).equalsIgnoreCase(URL_MODE_AS_IS)) {
            if (StringUtils.isNotBlank(expr)) {
                return getConnection(expr).getInputStream();
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
        }
    }

    @Override
    public void after() {
        super.after();
        IOUtils.closeQuietly(is);
    }
}
