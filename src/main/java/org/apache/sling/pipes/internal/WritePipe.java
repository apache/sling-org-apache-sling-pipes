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
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * pipe that writes to configured resource
 */
public class WritePipe extends BasePipe {
    private static final Logger logger = LoggerFactory.getLogger(WritePipe.class);
    public static final String RESOURCE_TYPE = RT_PREFIX + "write";
    protected static final Pattern ADD_PATCH = Pattern.compile("\\+\\[(.*)\\]");
    protected static final Pattern MULTI = Pattern.compile("\\[(.*)\\]");

    Node confTree;
    private List<Resource> propertiesToRemove;

    /**
     * public constructor
     * @param plumber plumber instance
     * @param resource configuration resource
     * @param upperBindings super pipe's bindings
     */
    public WritePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
    }

    Node getConfTree() {
        if (confTree == null) {
            if (getConfiguration() == null) {
                String pathCandidate = getExpr();
                if (StringUtils.isBlank(pathCandidate)) {
                    throw new IllegalArgumentException("write pipe is mis-configured: it should have a configuration node, or an expression");
                }
                Resource candidate = resolver.getResource(pathCandidate);
                if (candidate != null) {
                    return candidate.adaptTo(Node.class);
                } else {
                    throw new IllegalArgumentException("write pipe expression" + pathCandidate + " does not refer to any existing resource");
                }
            } else {
                confTree = getConfiguration().adaptTo(Node.class);
            }
        }
        return confTree;
    }


    /**
     * convert the configured string value (can be an expression) in a value that can be written in a resource.
     * also handles patch for multivalue properties like <code>+[value]</code> in which case <code>value</code>
     * is added to the MV property
     * @param resource resource to which value will be written
     * @param key property to which value will be written
     * @param expression configured value to write
     * @return actual value to write to the resource
     */
    protected Object computeValue(Resource resource, String key, String expression) {
        Object value = bindings.instantiateObject(expression);
        if (value instanceof String) {
            //in that case we treat special case like MV or patches
            String sValue = (String)value;
            Matcher patch = ADD_PATCH.matcher(sValue);
            if (patch.matches()) {
                String newValue = patch.group(1);
                String[] actualValues = resource.getValueMap().get(key, String[].class);
                List<String> newValues = actualValues != null ? new LinkedList<>(Arrays.asList(actualValues)) : new ArrayList<>();
                if (!newValues.contains(newValue)) {
                    newValues.add(newValue);
                }
                return newValues.toArray(new String[newValues.size()]);
            }
            Matcher multiMatcher = MULTI.matcher(sValue);
            if (multiMatcher.matches()) {
                return multiMatcher.group(1).split(",");
            }
        }
        return value;
    }

    /**
     * convert the configured string value (can be an expression) in a value that can be written in a resource.
     * also handles patch for multivalue properties like <code>+[value]</code> in which case <code>value</code>
     * is added to the MV property
     * @param resource resource to which value will be written
     * @param key property to which value will be written
     * @param expression configured value to write
     * @return actual value to write to the resource
     */
    protected Object computeValue(Resource resource, String key, Object expression)  {
        if (expression instanceof String) {
            return computeValue(resource, key, (String)expression);
        } else if (expression instanceof String[]){
            List<String> values = new ArrayList<>();
            for (String expr : (String[])expression){
                values.add((String)computeValue(resource, key, expr));
            }
            return values.toArray(new String[values.size()]);
        }
        return expression;
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    private void copyProperty(ModifiableValueMap targetProperties, Resource target, String key, Object value) {
        if (value == null) {
            //null value are not handled by modifiable value maps,
            //removing the property if it exists
            addPropertyToRemove(target.getChild(key));
        } else {
            logger.info("writing {}@{}={}", target.getPath(), key, value);
            if (!isDryRun()){
                targetProperties.put(key, value);
            }
        }
    }

    /**
     * Write properties from the configuration to the target resource,
     * instantiating both property names & values
     *
     * @param conf configured resource that holds all properties to write (and subpipes)
     * @param target target resource on which configured values will be written
     */
    private void copyProperties(@Nullable Resource conf, Resource target)  {
        ValueMap writeMap = conf != null ? conf.adaptTo(ValueMap.class) : null;
        ModifiableValueMap targetProperties = target.adaptTo(ModifiableValueMap.class);
        boolean modified = false;

        //writing current node
        if (properties != null && writeMap != null) {
            for (Map.Entry<String, Object> entry : writeMap.entrySet()) {
                if (!IGNORED_PROPERTIES.contains(entry.getKey())) {
                    String key = parent != null ? bindings.instantiateExpression(entry.getKey()) : entry.getKey();
                    Object value = computeValue(target, key, entry.getValue());
                    copyProperty(targetProperties, target, key, value);
                    modified = true;
                }
            }
        }
        if (modified) {
            plumber.markWithJcrLastModified(this, target);
        }
    }

    /**
     * we store all property to remove for very last moment (in order to potentially reuse their value)
     * @param property property resource that should be removed
     */
    private void addPropertyToRemove(Resource property){
        if (property != null) {
            if (propertiesToRemove == null) {
                propertiesToRemove = new ArrayList<>();
            }
            propertiesToRemove.add(property);
        }
    }

    private void copyNode(Node source, Node targetNode) throws RepositoryException {
        String name = source.getName();
        name = bindings.conditionalString(name);
        if (name == null) {
            logger.debug("name has been instantiated as null, not writing that tree");
        } else if (!isDryRun()){
            Node childTarget = targetNode.hasNode(name) ? targetNode.getNode(name) : targetNode.addNode(name, source.getPrimaryNodeType().getName());
            logger.debug("writing tree {}", childTarget.getPath());
            writeTree(source, resolver.getResource(childTarget.getPath()));
        }
    }

    /**
     * write the configured tree at the target resource, creating each node if needed, copying values.
     * @param conf configuration JCR tree to write to target resource
     * @param target target resource to write
     */
    private void writeTree(Node conf, Resource target) throws RepositoryException {
        copyProperties(resolver.getResource(conf.getPath()), target);
        NodeIterator childrenConf = conf.getNodes();
        Node targetNode = target.adaptTo(Node.class);
        if (childrenConf.hasNext() && targetNode != null) {
            logger.info("dubbing {} at {}", conf.getPath(), target.getPath());
            while (childrenConf.hasNext()) {
                Node node = childrenConf.nextNode();
                if (!IGNORED_NODES.contains(node.getName())) {
                    copyNode(node, targetNode);
                }
            }
        }
    }

    void managePropertiesToRemove() throws RepositoryException {
        if (propertiesToRemove != null && !propertiesToRemove.isEmpty()) {
            for (Resource propertyResource : propertiesToRemove) {
                logger.info("removing {}", propertyResource.getPath());
                if (!isDryRun()) {
                    Property property = propertyResource.adaptTo(Property.class);
                    if (property != null) {
                        property.remove();
                    }
                }
            }
        }
    }

    @Override
    protected Iterator<Resource> computeOutput() {
        try {
            Resource resource = getInput();
            if (resource != null) {
                writeTree(getConfTree(), resource);
                managePropertiesToRemove();
                return super.computeOutput();
            }
        } catch (RepositoryException e) {
            throw new IllegalStateException(e);
        } finally {
            if (propertiesToRemove != null){
                propertiesToRemove.clear();
            }
        }
        return EMPTY_ITERATOR;
    }
}
