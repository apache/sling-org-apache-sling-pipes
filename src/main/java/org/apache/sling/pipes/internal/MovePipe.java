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

    public MovePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        overwriteTarget = properties.get(PN_OVERWRITE_TARGET, false);
        orderBefore = properties.get(PN_ORDERBEFORE, false);
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    private Iterator<Resource> overwriteTarget(Resource input, Resource target) throws RepositoryException {
        logger.info("overwriting {}", target.getPath());
        Session session = resolver.adaptTo(Session.class);
        if (!isDryRun() && session != null) {
            Resource parent = target.getParent();
            String targetPath = target.getPath();
            Node targetParent = session.getItem(targetPath).getParent();
            if (targetParent.getPrimaryNodeType().hasOrderableChildNodes()) {
                String oldNodeName = target.getName();
                String targetPathNewNode = targetPath + UUID.randomUUID();
                String newNodeName = targetPathNewNode.substring(targetPathNewNode.lastIndexOf("/") + 1);
                session.move(input.getPath(), targetPathNewNode);
                targetParent.orderBefore(newNodeName, oldNodeName);
                session.removeItem(targetPath);
                // Need to use JackrabbitNode.rename() here, since session.move(targetPathNewNode, targetPath)
                // would move the new node back to the end of its siblings list
                JackrabbitNode newNode = (JackrabbitNode) session.getNode(targetPathNewNode);
                newNode.rename(oldNodeName);
                if (parent != null) {
                    return Collections.singleton(parent.getChild(oldNodeName)).iterator();
                }
            } else {
                session.removeItem(targetPath);
                session.move(input.getPath(), targetPath);
                if (parent != null) {
                    return Collections.singleton(parent.getChild(input.getName())).iterator();
                }
            }
        }
        return EMPTY_ITERATOR;
    }

    private Iterator<Resource> moveToTarget(Resource input, String targetPath) throws RepositoryException {
        logger.info("moving resource {} to {}", input.getPath(), targetPath);
        Session session = resolver.adaptTo(Session.class);
        if (!isDryRun() && session != null) {
            if (input.adaptTo(Node.class) != null) {
                session.move(input.getPath(), targetPath);
            } else {
                logger.debug("resource is a property");
                int lastLevel = targetPath.lastIndexOf("/");
                // /a/b/c will get cut in /a/b for parent path, and c for name
                String parentPath = targetPath.substring(0, lastLevel);
                String name = targetPath.substring(lastLevel + 1, targetPath.length());
                Property sourceProperty = input.adaptTo(Property.class);
                if (sourceProperty != null) {
                    Node destNode = session.getNode(parentPath);
                    if (sourceProperty.isMultiple()){
                        destNode.setProperty(name, sourceProperty.getValues(), sourceProperty.getType());
                    } else {
                        destNode.setProperty(name, sourceProperty.getValue(), sourceProperty.getType());
                    }
                    sourceProperty.remove();
                }
            }
            Resource target = resolver.getResource(targetPath);
            return Collections.singleton(target).iterator();
        }
        return EMPTY_ITERATOR;
    }

    private Iterator<Resource> reorder(Resource input, Resource target) throws RepositoryException, PersistenceException {
        if (target != null && !isDryRun()) {
            logger.info("ordering {} before {}", input.getPath(), target != null);
            Resource parent = target.getParent();
            if (parent != null) {
                Node targetParent = parent.adaptTo(Node.class);
                String oldNodeName = target.getName();
                if (targetParent != null && targetParent.getPrimaryNodeType().hasOrderableChildNodes()) {
                    String targetNodeName = ResourceUtil.createUniqueChildName(parent, input.getName());
                    String targetNodePath = targetParent.getPath() + SLASH + targetNodeName;
                    Session session = resolver.adaptTo(Session.class);
                    if (session != null) {
                        session.move(input.getPath(), targetNodePath);
                        targetParent.orderBefore(targetNodeName, oldNodeName);
                        return Collections.singleton(parent.getChild(targetNodeName)).iterator();
                    }
                } else {
                    logger.warn("parent resource {} doesn't support ordering", parent.getPath());
                }
            }
        }
        return EMPTY_ITERATOR;
    }

    @Override
    protected Iterator<Resource> computeOutput() {
        Iterator<Resource> output = EMPTY_ITERATOR;
        Resource input = getInput();
        if (input == null || input.adaptTo(Item.class) == null) {
            logger.error("bad configuration, input is either not here, or not something that can be moved");
            return output;
        }
        String targetPath = getExpr();
        if (! targetPath.startsWith(SLASH)) {
            logger.debug("relative path requested as target path: we'll take current path's parent");
            targetPath = StringUtils.substringBeforeLast(input.getPath(), SLASH) + SLASH + targetPath;
        }
        Resource targetResource = resolver.getResource(targetPath);
        try {
            if (targetResource == null) {
                if (orderBefore) {
                    logger.warn("target resource {} doesn't exist ordering not possible", targetPath);
                } else {
                    output = moveToTarget(input, targetPath);
                }
            } else {
                if (overwriteTarget) {
                    output = overwriteTarget(input, targetResource);
                } else if (orderBefore) {
                    output = reorder(input, targetResource);
                } else {
                    logger.warn("{} already exists, nothing will be done here, nothing outputed", targetPath);
                }
            }
        } catch (PersistenceException | RepositoryException e){
            logger.error("unable to move the resource", e);
        }
        return output;
    }
}
