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
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.sling.jcr.resource.JcrResourceConstants.NT_SLING_FOLDER;

/**
 * creates or get given expression's path and returns corresponding resource
 * this pipe can be configured with the following properties:
 * <ul>
 *     <li><code>nodeType</code> node type with which the leaf node of the created path will be created.
 *     Note that in that case Jackrabbit utilitary will be used, as this explicitely set path to be JCR.</li>
 *     <li><code>resourceType</code> resource type with which the leaf node of the created path will be created</li>
 *     <li><code>intermediateType</code> resource type with which intermediate nodse of the created path will be created</li>
 *     <li><code>autosave</code> flag indicating wether this pipe should triggers a commit at the end of the execution</li>
 * </ul>
 */
public class PathPipe extends BasePipe {

    public static final String RESOURCE_TYPE = RT_PREFIX + "path";
    public static final String PN_NODETYPE = "nodeType";
    public static final String PN_RESOURCETYPE = "resourceType";
    public static final String PN_INTERMEDIATE = "intermediateType";
    public static final String PN_AUTOSAVE = "autosave";

    String nodeType;
    String resourceType;
    String intermediateType;
    boolean autosave;

    private final Logger logger = LoggerFactory.getLogger(PathPipe.class);

    public PathPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        nodeType = properties.get(PN_NODETYPE, String.class);
        resourceType = properties.get(PN_RESOURCETYPE, NT_SLING_FOLDER);
        intermediateType = properties.get(PN_INTERMEDIATE, resourceType);
        autosave = properties.get(PN_AUTOSAVE, false);
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    @Override
    protected Iterator<Resource> computeOutput() {
        Iterator<Resource> output = Collections.emptyIterator();
        String expr = getExpr();
        try {
            String path = isRootPath(expr) ? expr : getInput().getPath() + SLASH + expr;
            logger.info("creating path {}", path);
            boolean modified = resolver.getResource(path) == null;
            if (!isDryRun()) {
                if (StringUtils.isNotBlank(nodeType)) {
                    //in that case we are in a "JCR" mode
                    JcrUtils.getOrCreateByPath(path, intermediateType, nodeType, resolver.adaptTo(Session.class), autosave);
                } else {
                    ResourceUtil.getOrCreateResource(resolver, path, resourceType, intermediateType, autosave);
                }
                Resource resource = resolver.getResource(path);
                if (modified && resource != null) {
                    plumber.markWithJcrLastModified(this, resource);
                }
                output = Collections.singleton(resource).iterator();
            }
        } catch (PersistenceException | RepositoryException e) {
            logger.error ("Not able to create path {}", expr, e);
        }
        return output;
    }
}
