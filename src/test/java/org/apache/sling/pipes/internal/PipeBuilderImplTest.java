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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.junit.Test;

public class PipeBuilderImplTest extends AbstractPipeTest {
    boolean fetchBooleanResource(String path) {
        Resource res = context.resourceResolver().getResource(path);
        if (res != null) {
            Boolean bool = res.adaptTo(Boolean.class);
            if (bool != null) {
                return bool;
            }
        }
        return false;
    }
    @Test
    public void testsimpleCreateResource() throws PersistenceException {
        PipeBuilderImpl impl = new PipeBuilderImpl(context.resourceResolver(), plumber);
        Map<String, Object> map = new HashMap<>();
        String rootPath = "/content/test";
        map.put("simple", true);
        Resource resource = impl.createResource(context.resourceResolver(), rootPath, "nt:unstructured", map);
        context.resourceResolver().commit();
        assertTrue(fetchBooleanResource(rootPath + "/simple"));
    }

    @Test
    public void testcreateResource() throws PersistenceException {
        PipeBuilderImpl impl = new PipeBuilderImpl(context.resourceResolver(), plumber);
        Map<String, Object> map = new HashMap<>();
        String rootPath = "/content/test";
        map.put("one/levelDepth", true);
        map.put("one/anotherLevel/depth", true);
        Resource resource = impl.createResource(context.resourceResolver(), rootPath, "nt:unstructured", map);
        assertNotNull(resource);
        assertEquals("returned resource should be the shallower resource where we wrote", rootPath + "/one", resource.getPath());
        context.resourceResolver().commit();
        assertTrue(fetchBooleanResource(rootPath + "/one/levelDepth"));
        assertTrue(fetchBooleanResource(rootPath + "/one/anotherLevel/depth"));
    }

    @Test
    public void testLocalBindings() throws PersistenceException, InvocationTargetException, IllegalAccessException {
        plumber.newPipe(context.resourceResolver()).echo("/content/fruits/${fruit}").build("/some/reference");
        ExecutionResult result = execute("ref /some/reference @ bindings fruit=apple");
        assertEquals(1, result.size());
        assertEquals("/content/fruits/apple", result.getCurrentPathSet().iterator().next());
    }
}
