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

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.Pipe;
import org.junit.Test;

import javax.jcr.Node;

import static org.apache.sling.jcr.resource.JcrResourceConstants.NT_SLING_FOLDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Calendar;

/**
 * Testing path pipe using pipe builder
 */
public class PathPipeTest extends AbstractPipeTest {

    private static final String WATERMELON = "watermelon";
    private static final String WATERMELON_RELATIVEPATH = "parent/" + WATERMELON;
    private static final String WATERMELON_FULL_PATH = PATH_FRUITS + "/" + WATERMELON_RELATIVEPATH;

    @Test
    public void modifiesContent() throws PersistenceException {
        Pipe pipe = plumber.newPipe(context.resourceResolver())
                .mkdir(PATH_FRUITS + "/whatever")
                .build();
        assertTrue("path pipe should be considered as modifying the content", pipe.modifiesContent());
    }

    @Test
    public void getClassicOutputResource() throws Exception {
        ResourceResolver resolver = context.resourceResolver();
        plumber.newPipe(resolver).mkdir(WATERMELON_FULL_PATH).run();
        assertNotNull("Resource should be here & saved", resolver.getResource(WATERMELON_FULL_PATH));
    }

    @Test
    public void getClassicOutputJCR() throws Exception {
        ResourceResolver resolver = context.resourceResolver();
        plumber.newPipe(resolver).mkdir(WATERMELON_FULL_PATH).with("resourceType","nt:unstructured","intermediateType",NT_SLING_FOLDER).run();
        Resource watermelon = resolver.getResource(WATERMELON_FULL_PATH);
        assertNotNull("Resource should be here & saved", watermelon);
        Node node = watermelon.adaptTo(Node.class);
        assertEquals("node type should be nt:unstructured", "nt:unstructured",node.getPrimaryNodeType().getName());
        assertEquals("Parent node type should be sling:Folder", NT_SLING_FOLDER, node.getParent().getPrimaryNodeType().getName());
    }

    @Test
    public void getRelativePath() throws Exception {
        ResourceResolver resolver = context.resourceResolver();
        plumber.newPipe(resolver).echo(PATH_FRUITS).mkdir(WATERMELON_RELATIVEPATH).run();
        assertNotNull("Resource should be here & saved", resolver.getResource(WATERMELON_FULL_PATH));
    }
    @Test
    public void testJcrMark() throws InvocationTargetException, IllegalAccessException {
        Instant now = Instant.now();
        String path = "/content/my/new/path";
        execute("mkdir " + path);
        ValueMap fruits = context.resourceResolver().getResource(path).adaptTo(ValueMap.class);
        assertNotNull(fruits.get("jcr:lastModified", Calendar.class));
        Instant modified = Instant.ofEpochMilli(fruits.get("jcr:lastModified", Calendar.class).getTimeInMillis());
        assertTrue(modified.isAfter(now));
        assertNotNull(fruits.get("jcr:lastModifiedBy", String.class));
        //we configured the plumber to mark pipe path
        assertNotNull(fruits.get("jcr:lastModifiedByPipe", String.class));
        execute("mkdir " + path);
        fruits = context.resourceResolver().getResource(path).adaptTo(ValueMap.class);
        Instant modifiedAgain = Instant.ofEpochMilli(fruits.get("jcr:lastModified", Calendar.class).getTimeInMillis());
        assertEquals("path should not mark *again* a path already created", modified, modifiedAgain);
    }
    
}