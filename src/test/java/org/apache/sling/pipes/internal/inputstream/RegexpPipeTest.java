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
package org.apache.sling.pipes.internal.inputstream;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.Pipe;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RegexpPipeTest extends AbstractPipeTest {
    @Test
    public void getGroupNames() throws Exception {
        RegexpPipe pipe = new RegexpPipe(plumber, context.resourceResolver().getResource("/content"), null);
        Collection<String> names = pipe.getGroupNames("some (?<first>group) that uses (?<name>names)");
        assertEquals("there should be 2 names", 2, names.size());
        assertTrue("there should be first", names.contains("first"));
        assertTrue("there should be name", names.contains("name"));
    }

    @Test
    public void getOutputWithSimpleMatch() throws Exception {
        String htmlPath = "/content/test/standardTest.html";
        context.load().binaryFile("/standardTest.html", htmlPath);
        Pipe pipe = plumber.newPipe(context.resourceResolver())
                .echo("/content")
                .egrep(htmlPath).name("location").with("pattern","http://www.apache[^\\s]+")
                .write("urls","+[${location}]").build();
        Iterator<Resource> output = pipe.getOutput();
        output.next();
        Resource result = context.resourceResolver().getResource("/content/urls");
        assertNotNull("there should be a result property", result);
        String[] aUrls = result.adaptTo(String[].class);
        assertNotNull("result property should be a MV", aUrls);
        List<String> urls = Arrays.asList(aUrls);
        assertEquals("there should be 1 elements", 1, urls.size());
        assertEquals("first should be http://www.apache.org/licenses/LICENSE-2.0", "http://www.apache.org/licenses/LICENSE-2.0", urls.get(0));
    }

    @Test
    public void getOutputWithNames() throws Exception {
        String htmlPath = "/content/test/standardTest.html";
        context.load().binaryFile("/standardTest.html", htmlPath);
        Pipe pipe = plumber.newPipe(context.resourceResolver())
                .echo("/content")
                .egrep(htmlPath).name("location").with("pattern","\"(?<domain>http://[^/]+)(?<uri>[^\"^\']+)\"")
                .mkdir("${location.uri}")
                .write("domain","${location.domain}").build();
        Iterator<Resource> output = pipe.getOutput();
        List<Resource> resources = IteratorUtils.toList(output);
        List<String> paths = resources.stream().map( resource -> resource.getPath()).collect(Collectors.toList());
        assertEquals("there should be 3 elements", 3, paths.size());
        assertEquals("first should be /content/img/1.png", "/content/img/1.png", paths.get(0));
        assertEquals("second should be /content/page.html", "/content/page.html", paths.get(1));
        assertEquals("third should be /content/img/2.png", "/content/img/2.png", paths.get(2));
        assertEquals("one created resource's domain should be somesite", "http://somesite.com",
                context.resourceResolver().getResource("/content/img/1.png/domain").adaptTo(String.class));
    }

    @Test
    public void testRegexAsIsUrl() throws Exception {
        Pipe pipe = plumber.newPipe(context.resourceResolver())
                .echo("/content")
                .egrep("https://sling.apache.org/documentation/bundles/sling-pipes/readers.html")
                .name("result").with("pattern","(?<domain>https?://[^/]+)(?<uri>[^\"^\']+)")
                .with("url_mode","as_is").build();
        Iterator<Resource> output = pipe.getOutput();
        IteratorUtils.toList(output);
        assertEquals("domain should be read from the egrep expression as is", "https://sling.apache.org",pipe.getBindings().instantiateExpression("${result.domain}"));
        assertEquals("uri should be read from the egrep expression as is", "/documentation/bundles/sling-pipes/readers.html",pipe.getBindings().instantiateExpression("${result.uri}"));
    }

    @Test
    public void readThePatternFromTheBindingsIfAvailable() throws Exception {
        String htmlPath = "/content/test/standardTest.html";
        context.load().binaryFile("/standardTest.html", htmlPath);
        Pipe pipe = plumber.newPipe(context.resourceResolver())
                .echo("/content")
                .json("{'items': [{'pattern': '(?<domain>http://somesite.com/[^/]+)(?<uri>.*/1.png)'}]}").with("valuePath", "$.items").name("json")
                .egrep(htmlPath).name("location").with("pattern","${json.pattern}")
                .write("test","${location.uri}").build();
        Iterator<Resource> output = pipe.getOutput();
        output.next();
        Resource result = context.resourceResolver().getResource("/content/test");
        assertNotNull("there should be a test property", result);
        assertEquals("resource should have test property with value /img/1.png ", "/img/1.png",
                context.resourceResolver().getResource("/content/test").adaptTo(String.class));
    }

    @Test
    public void validatePartiallyValidBindingExpr() throws Exception {
        String htmlPath = "/content/test/standardTest.html";
        context.load().binaryFile("/standardTest.html", htmlPath);
        Pipe pipe = plumber.newPipe(context.resourceResolver())
                .echo("/content")
                .json("{'items': [{'pattern': '(?<domain>http://somesite.com/[^/]+)(?<uri>.*/1.png)'}]}").with("valuePath", "$.items").name("json")
                .egrep(htmlPath).name("location").with("pattern","${json.invalid}")
                .write("test","${location.uri}").build();
        Iterator<Resource> output = pipe.getOutput();
        assertFalse("pipe executed with empty output", output.hasNext());
    }
}
