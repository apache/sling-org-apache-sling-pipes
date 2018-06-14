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
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;

import static org.apache.sling.jcr.resource.api.JcrResourceConstants.NT_SLING_FOLDER;

/**
 * creates or get given expression's path and returns corresponding resource
 * this pipe can be configured with the following properties:
 * <ul>
 *     <li><code>nodeType</code> resource type with which the leaf node of the created path will be created</li>
 *     <li><code>intermediateType</code> resource type with which intermediate nodse of the created path will be created</li>
 *     <li><code>autosave</code> flag indicating wether this pipe should triggers a commit at the end of the execution</li>
 * </ul>
 */
public class PathPipe extends BasePipe {

    public static final String RESOURCE_TYPE = RT_PREFIX + "path";
    public static final String PN_RESOURCETYPE = "resourceType";
    public static final String PN_NODETYPE = "nodeType";
    public static final String PN_INTERMEDIATE = "intermediateType";
    public static final String PN_AUTOSAVE = "autosave";

    String resourceType;
    String nodeType;
    String intermediateType;
    boolean autosave;
    boolean jcr;

    private final Logger logger = LoggerFactory.getLogger(PathPipe.class);

    public PathPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) throws Exception {
        super(plumber, resource, upperBindings);
        nodeType = properties.get(PN_NODETYPE, String.class);
        resourceType = properties.get(PN_RESOURCETYPE, NT_SLING_FOLDER);
        jcr = StringUtils.isNotBlank(nodeType);
        intermediateType = properties.get(PN_INTERMEDIATE, resourceType);
        autosave = properties.get(PN_AUTOSAVE, false);
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    @Override
    protected Iterator<Resource> computeOutput() throws Exception {
        Iterator<Resource> output = Collections.emptyIterator();
        String expr = getExpr();
        try {
            String path = isRootPath(expr) ? expr : getInput().getPath() + SLASH + expr;
            logger.info("creating path {}", path);
            if (!isDryRun()) {
                Resource resource = jcr ? getOrCreateNode(path) : ResourceUtil.getOrCreateResource(resolver, path, resourceType, intermediateType, autosave);
                output = Collections.singleton(resource).iterator();
            }
        } catch (PersistenceException e){
            logger.error ("Not able to create path {}", expr, e);
        }
        return output;
    }

    /**
     * get or create JCR path, using pipe members
     * @param path path to create
     * @return resource corresponding to the created leaf
     * @throws RepositoryException in case something went wrong with jcr creation
     */
    protected Resource getOrCreateNode(String path) throws RepositoryException {
        Node leaf = null;
        boolean transientChange = false;
        String relativePath = path.substring(1);
        Node parentNode = resolver.adaptTo(Session.class).getRootNode();
        if (!parentNode.hasNode(relativePath)) {
            Node node = parentNode;
            int pos = relativePath.lastIndexOf('/');
            if (pos != -1) {
                final StringTokenizer st = new StringTokenizer(relativePath.substring(0, pos), "/");
                while (st.hasMoreTokens()) {
                    final String token = st.nextToken();
                    if (!node.hasNode(token)) {
                        try {
                            node.addNode(token, intermediateType);
                            transientChange = true;
                        } catch (RepositoryException re) {
                            // we ignore this as this folder might be created from a different task
                            node.getSession().refresh(false);
                        }
                    }
                    node = node.getNode(token);
                }
                relativePath = relativePath.substring(pos + 1);
            }
            if (!node.hasNode(relativePath)) {
                node.addNode(relativePath, nodeType);
                transientChange = true;
            }
            leaf = node.getNode(relativePath);
        }
        if (leaf == null) {
            leaf = parentNode.getNode(relativePath);
        }
        if (transientChange && autosave) {
            resolver.adaptTo(Session.class).save();
        }
        return resolver.getResource(leaf.getPath());
    }
}
