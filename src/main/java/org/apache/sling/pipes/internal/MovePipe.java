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

import org.apache.jackrabbit.api.JackrabbitNode;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

/**
 * Does a JCR Move of a node, returns the resource corresponding to the moved node
 */
public class MovePipe extends BasePipe {
    Logger logger = LoggerFactory.getLogger(MovePipe.class);

    public static final String RESOURCE_TYPE = RT_PREFIX + "mv";
    public static final String PN_OVERWRITE_TARGET = "overwriteTarget";
    public static final String PN_ORDERBEFORE = "orderBeforeTarget";

    private boolean overwriteTarget;
    private boolean orderBefore;

    public MovePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) throws Exception {
        super(plumber, resource, upperBindings);
        overwriteTarget = properties.get(PN_OVERWRITE_TARGET, false);
        orderBefore = properties.get(PN_ORDERBEFORE, false);
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    @Override
    protected Iterator<Resource> computeOutput() throws Exception {
        Iterator<Resource> output = Collections.emptyIterator();
        Resource resource = getInput();
        if (resource != null && resource.adaptTo(Item.class) != null) {
            String targetPath = getExpr();
            try {
                Session session = resolver.adaptTo(Session.class);
                if (session.itemExists(targetPath)) {
                    //if target item exist then either it should overwrite or order the source before the target
                    if (orderBefore && !isDryRun()) {
                        output = reorder(resource, targetPath, session);
                    } else if (overwriteTarget && !isDryRun()) {
                        output = overwriteTargetNode(resource, targetPath, session);
                    } else {
                        logger.warn("{} already exists, nothing will be done here, nothing outputed");
                    }
                } else {
                    logger.info("moving resource {} to {}", resource.getPath(), targetPath);
                    if (!isDryRun()) {
                        if (resource.adaptTo(Node.class) != null) {
                            session.move(resource.getPath(), targetPath);
                        } else {
                            logger.debug("resource is a property");
                            int lastLevel = targetPath.lastIndexOf("/");
                            // /a/b/c will get cut in /a/b for parent path, and c for name
                            String parentPath = targetPath.substring(0, lastLevel);
                            String name = targetPath.substring(lastLevel + 1, targetPath.length());
                            Property sourceProperty = resource.adaptTo(Property.class);
                            Node destNode = session.getNode(parentPath);
                            if (sourceProperty.isMultiple()){
                                destNode.setProperty(name, sourceProperty.getValues(), sourceProperty.getType());
                            } else {
                                destNode.setProperty(name, sourceProperty.getValue(), sourceProperty.getType());
                            }
                            sourceProperty.remove();
                        }
                        Resource target = resolver.getResource(targetPath);
                        output = Collections.singleton(target).iterator();
                    }
                }
            } catch (RepositoryException e){
                logger.error("unable to move the resource", e);
            }
        } else {
            logger.warn("bad configuration of the pipe, will do nothing");
        }
        return output;
    }

    private Iterator<Resource> overwriteTargetNode(Resource resource, String targetPath, Session session) throws Exception {
        logger.debug("overwriting {}", targetPath);
        Resource parent = resolver.getResource(targetPath).getParent();
        Node targetParent = session.getItem(targetPath).getParent();
        String oldNodeName = targetPath.substring(targetPath.lastIndexOf("/") + 1);
        String targetPathNewNode = targetPath + UUID.randomUUID();
        String newNodeName = targetPathNewNode.substring(targetPathNewNode.lastIndexOf("/") + 1);
        if (targetParent.getPrimaryNodeType().hasOrderableChildNodes()) {
            session.move(resource.getPath(), targetPathNewNode);
            targetParent.orderBefore(newNodeName, oldNodeName);
            session.removeItem(targetPath);
            // Need to use JackrabbitNode.rename() here, since session.move(targetPathNewNode, targetPath)
            // would move the new node back to the end of its siblings list
            JackrabbitNode newNode = (JackrabbitNode) session.getNode(targetPathNewNode);
            newNode.rename(oldNodeName);
            return Collections.singleton(parent.getChild(oldNodeName)).iterator();
        } else {
            session.removeItem(targetPath);
            session.move(resource.getPath(), targetPath);
            return Collections.singleton(parent.getChild(resource.getName())).iterator();
        }
    }

    private Iterator<Resource> reorder(Resource resource, String targetPath, Session session) throws Exception {
        logger.debug("ordering {} before {}", resource.getPath(), targetPath);
        Resource parent = resolver.getResource(targetPath).getParent();
        Node targetParent = session.getItem(targetPath).getParent();
        String oldNodeName = targetPath.substring(targetPath.lastIndexOf("/") + 1);
        if (targetParent.getPrimaryNodeType().hasOrderableChildNodes()) {
            String targetNodeName = ResourceUtil.createUniqueChildName(parent, resource.getName());
            String targetNodePath = targetParent.getPath() + SLASH + targetNodeName;
            session.move(resource.getPath(), targetNodePath);
            targetParent.orderBefore(targetNodeName, oldNodeName);
            return Collections.singleton(parent.getChild(targetNodeName)).iterator();
        } else {
            logger.warn("parent resource {} doesn't support ordering", parent.getPath());
        }
        return EMPTY_ITERATOR;
    }
}
