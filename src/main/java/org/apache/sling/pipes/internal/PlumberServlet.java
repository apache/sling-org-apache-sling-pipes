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

import static org.apache.sling.pipes.internal.PlumberServlet.RESOURCE_TYPE;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.ServletResolverConstants;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.internal.slingquery.ChildrenPipe;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Map;

/**
 * Servlet executing plumber for a pipe path given as 'path' parameter,
 * it can also be launched against a container pipe resource directly (no need for path parameter)
 */
@Component(service = {Servlet.class},
        property= {
                ServletResolverConstants.SLING_SERVLET_METHODS + "=GET",
                ServletResolverConstants.SLING_SERVLET_METHODS + "=POST",
                ServletResolverConstants.SLING_SERVLET_EXTENSIONS + "=json",
                ServletResolverConstants.SLING_SERVLET_EXTENSIONS + "=csv"
        })
@Designate(ocd = PlumberServlet.Configuration.class)
public class PlumberServlet extends AbstractPlumberServlet {
    public static final String RESOURCE_TYPE = "slingPipes/plumber";

    static final String PARAM_PATH = "path";

    static final String PARAM_BINDINGS = "bindings";

    static final String PARAM_ASYNC = "async";

    @Reference
    Plumber plumber;

    boolean enabled = false;

    @Activate
    @Modified
    public void activate(Configuration configuration) {
        enabled = configuration.enabled();
    }

    @ObjectClassDefinition(name="Apache Sling Pipes : Plumber Servlet Configuration")
    public @interface Configuration {
        @AttributeDefinition(description="Enable servlet to execute pipe, if disabled will return 503")
        boolean enabled() default true;

        @AttributeDefinition(description="Resource types with which that servlet can be called")
        String[] sling_servlet_resourceTypes() default { RESOURCE_TYPE, ContainerPipe.RESOURCE_TYPE, 
            ManifoldPipe.RESOURCE_TYPE, AuthorizablePipe.RESOURCE_TYPE,
            WritePipe.RESOURCE_TYPE, ChildrenPipe.RESOURCE_TYPE };
    }

    @Override
    protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response) throws ServletException, IOException {
        if (enabled) {
            if (Arrays.asList(request.getRequestPathInfo().getSelectors()).contains(BasePipe.PN_STATUS)){
                response.getWriter().append(plumber.getStatus(request.getResource()));
            } else {
                execute(request, response, false);
            }
        } else {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "http service has been disabled");
        }
    }

    @Override
    protected void doPost(SlingHttpServletRequest request, SlingHttpServletResponse response) throws ServletException, IOException {
        if (enabled) {
            execute(request, response, true);
        } else {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "http service has been disabled");
        }
    }

    /**
     * Execution of a pipe corresponding to a request
     * @param request original request
     * @param response given response
     * @param writeAllowed should we consider this execution is about to modify content
     * @throws ServletException in case something is wrong...
     */
    void execute(SlingHttpServletRequest request, SlingHttpServletResponse response, boolean writeAllowed) throws ServletException {
        String path = request.getResource().getResourceType().equals(RESOURCE_TYPE) ? request.getParameter(PARAM_PATH) : request.getResource().getPath();
        try {
            if (StringUtils.isBlank(path)) {
                throw new IllegalArgumentException("path should be provided");
            }
            Map<String, Object> bindings = plumber.getBindingsFromRequest(request, writeAllowed);
            String asyncParam = request.getParameter(PARAM_ASYNC);
            if (StringUtils.isNotBlank(asyncParam) && asyncParam.equals(Boolean.TRUE.toString())){
                Job job = plumber.executeAsync(request.getResourceResolver(), path, bindings);
                if (job != null){
                    response.getWriter().append("pipe execution registered as " + job.getId());
                    response.setStatus(HttpServletResponse.SC_CREATED);
                } else {
                    response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Some issue with your request, or server not being ready for async execution");
                }
            } else {
                OutputWriter writer = getWriter(request, response);
                plumber.execute(request.getResourceResolver(), path, bindings, writer, true);
            }
        }
        catch (AccessControlException e) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        }
        catch (Exception e) {
            throw new ServletException(e);
        }
    }
}