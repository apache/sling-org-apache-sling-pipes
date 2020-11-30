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
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Ignore
public class PackagePipeTest extends AbstractPipeTest {

    @Rule
    public SlingContext oak = new SlingContext(ResourceResolverType.JCR_OAK);

    @Test
    public void filterModeTest() throws Exception {
        oak.load().json("/initial-content/content/fruits.json", PATH_FRUITS);
        String packagePath = "/content/package";
        ExecutionResult result = execute(oak.resourceResolver(),
                "echo /content/fruits | children nt:unstructured | package /content/package");
        assertTrue("there should be more than one output", result.size() > 0);
        Resource packageResource = context.resourceResolver().getResource(packagePath);
        assertNotNull(packageResource);
    }
}