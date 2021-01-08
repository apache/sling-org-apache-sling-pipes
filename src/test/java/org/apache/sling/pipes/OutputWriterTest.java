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
package org.apache.sling.pipes;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.testing.mock.sling.servlet.MockSlingHttpServletRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OutputWriterTest extends AbstractPipeTest {
    OutputWriter writer = new OutputWriter() {
        @Override
        public boolean handleRequest(SlingHttpServletRequest request) {
            return false;
        }

        @Override
        protected void initResponse(SlingHttpServletResponse response) {

        }

        @Override
        public void starts() {

        }

        @Override
        protected void writeItem(Resource resource) {

        }

        @Override
        public void ends() {

        }
    };



    public void initWithParam(String key, String value) throws IOException {
        MockSlingHttpServletRequest request = context.request();
        Map<String,Object> map = new HashMap<>();
        map.put(key,value);
        request.setParameterMap(map);
        writer.init(request, context.response());
    }

    @Test
    public void testSize() throws IOException {
        initWithParam("size","19");
        assertEquals(19, writer.max);
    }

    @Test
    public void testCustomWriter() throws IOException {
        initWithParam("writer","one=1,two=2,three=3");
        assertFalse(writer.customOutputs.isEmpty());
        assertEquals("${1}", writer.customOutputs.get("one"));
    }
}