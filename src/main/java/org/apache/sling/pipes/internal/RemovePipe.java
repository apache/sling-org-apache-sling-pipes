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

import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;

import java.util.Collections;
import java.util.Iterator;

/**
 * this pipe tries to remove the input resource, abstracting its type,
 * returning parent of the input
 */
public class RemovePipe extends BasePipe {
    private static Logger logger = LoggerFactory.getLogger(RemovePipe.class);
    public static final String RESOURCE_TYPE = RT_PREFIX + "rm";

    /**
     * In case input resource is a node and configuration is set, only configured properties,
     * and subtrees will be removed
     */
    Resource filter;

    public RemovePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        filter = getConfiguration();
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    @Override
    protected Iterator<Resource> computeOutput() {
        Resource resource = getInput();
        String parentPath = null;
        try {
            if (resource.adaptTo(Node.class) != null) {
                parentPath = removeTree(resource, filter);
            } else {
                Property property = resource.adaptTo(Property.class);
                if (property != null){
                    Node parent = property.getParent();
                    if (parent != null) {
                        parentPath = parent.getPath();
                        logger.info("removing property {}", property.getPath());
                        if (!isDryRun()){
                            property.remove();
                        }
                    }
                }
            }
            if (parentPath != null) {
                return Collections.singleton(resolver.getResource(parentPath)).iterator();
            }
        } catch (RepositoryException e) {
            logger.error("unable to remove given resource", e);
        }
        return Collections.emptyIterator();
    }

    private boolean removeProperty(@NotNull Node node, String key) throws RepositoryException {
        if (! IGNORED_PROPERTIES.contains(key) && node.hasProperty(key)){
            logger.info("removing property {}", node.getProperty(key).getPath());
            if (!isDryRun()){
                node.getProperty(key).remove();
                return true;
            }
        }
        return false;
    }
    /**
     * remove properties, returns the number of properties that were configured to be removed
     * @return
     */
    private int removeProperties(Resource resource, Resource configuration) throws RepositoryException {
        int count = 0;
        if (configuration != null) {
            Node node = resource.adaptTo(Node.class);
            if (node != null) {
                ValueMap configuredProperties = configuration.getValueMap();
                for (String key : configuredProperties.keySet()){
                    if (removeProperty(node, key)) {
                        count ++;
                    }
                }
            }
        }
        return count;
    }

    private String removeNode(Resource resource) throws RepositoryException {
        //explicit configuration to remove the node altogether
        logger.info("removing node {}", resource.getPath());
        Resource parent = resource.getParent();
        if (parent != null && !isDryRun()){
            Node node = resource.adaptTo(Node.class);
            if (node != null) {
                node.remove();
            }
            return parent.getPath();
        }
        return resource.getPath();
    }

    private String removeTree(Resource resource, Resource configuration) throws RepositoryException {
        logger.debug("removing tree {}", resource.getPath());
        String remainingPath = resource.getPath();
        int configuredProperties = removeProperties(resource, configuration);
        Node configuredNode = configuration != null ? configuration.adaptTo(Node.class) : null;
        NodeIterator childConf = configuredNode != null ? configuredNode.getNodes() : null;
        if (childConf == null || (! childConf.hasNext() && configuredProperties == 0)){
            remainingPath = removeNode(resource);
        } else {
            while (childConf.hasNext()) {
                Node childToRemove = childConf.nextNode();
                Resource child = resource.getChild(childToRemove.getName());
                if (child != null){
                    removeTree(child, configuration.getChild(childToRemove.getName()));
                }
            }
        }
        return remainingPath;
    }
}
