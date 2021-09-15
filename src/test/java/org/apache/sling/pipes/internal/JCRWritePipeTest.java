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

import static org.apache.sling.testing.mock.caconfig.ContextPlugins.CACONFIG;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.apache.sling.testing.mock.sling.junit.SlingContextBuilder;
import org.codehaus.plexus.util.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JCRWritePipeTest extends AbstractPipeTest {

    @Rule
    public SlingContext jcrContext = new SlingContextBuilder(ResourceResolverType.JCR_OAK).plugin(CACONFIG).build();

    @Before
    public void setup() throws PersistenceException {
        context = jcrContext;
        super.setup();
    }

    void assertMixinTypesEquals(String path, String... mixins) throws RepositoryException {
        String[] m = mixins != null ? mixins : new String[0];
        Resource resource = context.resourceResolver().getResource(path);
        Node node = resource.adaptTo(Node.class);
        Collection<String> intersection = CollectionUtils.intersection(Arrays.asList(m),
                Arrays.stream(node.getMixinNodeTypes())
                        .map(NodeType::getName)
                        .collect(Collectors.toList()));
        assertEquals(m.length, intersection.size());
    }

    @Test
    public void testMixins() throws InvocationTargetException, IllegalAccessException, RepositoryException {
        execute("mkdir /content/typed");
        assertMixinTypesEquals("/content/typed");
        execute("echo /content/typed | write jcr:mixinTypes=[sling:HierarchyNode,sling:Resource]");
        assertMixinTypesEquals("/content/typed","sling:HierarchyNode", "sling:Resource");
        execute("echo /content/typed | write jcr:mixinTypes=[sling:Resource]");
        assertMixinTypesEquals("/content/typed", "sling:Resource");
    }
}
