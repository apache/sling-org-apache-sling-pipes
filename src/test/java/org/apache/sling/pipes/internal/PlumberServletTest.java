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
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.request.RequestPathInfo;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.ContainerPipeTest;
import org.apache.sling.pipes.OutputWriter;
import org.junit.Before;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.servlet.ServletException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * testing the servlet logic (parameters & output)
 */
public class PlumberServletTest extends AbstractPipeTest {

    static final int DUMMYTREE_TEST_SIZE = 4;

    String containersPath = PATH_PIPE + "/" + "containers";

    String dummyTreePath = containersPath + "/" + ContainerPipeTest.NN_DUMMYTREE;

    String writePath = PATH_PIPE + "/" + "write";

    String pipedWritePath = writePath + "/" + WritePipeTest.NN_PIPED;

    StringWriter stringResponse;

    SlingHttpServletResponse response;

    PlumberServlet servlet = new PlumberServlet();

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        context.load().json("/plumber.json", PATH_PIPE);
        context.load().json("/container.json", containersPath);
        context.load().json("/write.json", writePath);
        servlet.plumber = plumber;
        servlet.enabled = true;
        stringResponse = new StringWriter();
        try {
            response = mockPlumberServletResponse(stringResponse);
        } catch (Exception e){

        }
    }

    private void assertDummyTree(int size) {
        String finalResponse = stringResponse.toString();
        assertFalse("There should be a response", StringUtils.isBlank(finalResponse));
        JsonObject object = Json.createReader(new StringReader(finalResponse)).readObject();
        assertEquals("response should be an obj with size value equals to " + DUMMYTREE_TEST_SIZE, object.getInt(OutputWriter.KEY_SIZE), DUMMYTREE_TEST_SIZE);
        assertEquals("response should be an obj with items value equals to a " + size + " valued array", object.getJsonArray(OutputWriter.KEY_ITEMS).size(), size);
    }

    private void assertDummyTree()  {
        assertDummyTree(DUMMYTREE_TEST_SIZE);
    }

    @Test
    public void testDummyTreeThroughRT() throws Exception {
        SlingHttpServletRequest request = mockPlumberServletRequest(context.resourceResolver(), "json", dummyTreePath, null, null, null, null, null);
        servlet.execute(request, response, false);
        assertDummyTree();
    }

    @Test
    public void testDummyTreeThroughPlumber() throws Exception {
        SlingHttpServletRequest request = mockPlumberServletRequest(context.resourceResolver(), "json", PATH_PIPE, dummyTreePath, null, null, null, null);
        servlet.execute(request, response, false);
        assertDummyTree();
    }

    @Test
    public void testWriteExecute() throws ServletException {
        SlingHttpServletRequest request = mockPlumberServletRequest(context.resourceResolver(), "json", pipedWritePath, null, null, null, null, null);
        servlet.execute(request, response, true);
        String finalResponse = stringResponse.toString();
        assertFalse("There should be a response", StringUtils.isBlank(finalResponse));
        assertFalse("There should be no more pending changes", context.resourceResolver().hasChanges());
    }

    /**
     * in this test we execute a pipe that modifies content, with a flag mocking the GET request:
     * the execution should fail.
     */
    @Test
    public void testGetOnWriteExecute() {
        SlingHttpServletRequest request = mockPlumberServletRequest(context.resourceResolver(), "json", pipedWritePath, null, null, null, null, null);
        boolean hasFailed = true;
        try {
            servlet.execute(request, response, false);
            hasFailed = false;
        } catch (Exception e){

        }
        assertTrue("Execution should have failed", hasFailed);
    }

    /**
     * in this test we execute a pipe that modifies content, with a flag mocking the GET request, but with a dryRun parameter
     * the execution should *not* fail.
     */
    @Test
    public void testGetOnOnDryRUnWriteExecute() {
        SlingHttpServletRequest request = mockPlumberServletRequest(context.resourceResolver(), "json", pipedWritePath, null, null, null, "true", "-1");
        boolean hasFailed = true;
        try {
            servlet.execute(request, response, false);
            hasFailed = false;
        } catch (Exception e){}
        assertFalse("Execution should not have failed", hasFailed);
    }

    @Test
    public void testAdditionalBindingsAndWriter() throws Exception {
        String bindings = "{\"testBinding\":\"testBindingValue\"}";
        String respObject =  "pathLength=path.get(\"dummyGrandChild\").length(),testBindingValue=testBinding.length()";
        SlingHttpServletRequest request =
                mockPlumberServletRequest(context.resourceResolver(), "json", dummyTreePath, null, bindings, respObject, null, null);
        servlet.execute(request, response, false);
        assertDummyTree();
        JsonObject response = Json.createReader(new StringReader(stringResponse.toString())).readObject();
        JsonArray array = response.getJsonArray(OutputWriter.KEY_ITEMS);
        for (int i = 0; i < array.size(); i++) {
            JsonObject object = array.getJsonObject(i);
            assertNotNull("there should be an object returned at each time", object);
            String path = object.getString(OutputWriter.PATH_KEY);
            assertNotNull("the string path should be returned for each item, containing the path of the resource");
            String pathLength = object.getString("pathLength");
            assertNotNull("there should be a pathLength param, as specified in the writer", pathLength);
            assertEquals("Pathlength should be the string representation of the path length", path.length() + "", pathLength);
            String testBindingLengthValue = object.getString("testBindingValue", null);
            assertNotNull("testBindingLength should be there", testBindingLengthValue);
            assertEquals("testBindingLength should be the string representation of the additional binding length",
                    "testBindingValue".length() + "", testBindingLengthValue);
        }
    }

    @Test
    public void testDryRun() throws Exception {
        SlingHttpServletRequest dryRunRequest =
                mockPlumberServletRequest(context.resourceResolver(), "json", pipedWritePath, null, null, null, "true", null);
        servlet.execute(dryRunRequest, response, true);
        Resource resource = context.resourceResolver().getResource("/content/fruits");
        ValueMap properties = resource.adaptTo(ValueMap.class);
        assertFalse("property fruits shouldn't have been written", properties.containsKey("fruits"));
        SlingHttpServletRequest request =
                mockPlumberServletRequest(context.resourceResolver(), "json", pipedWritePath, null, null, null, "false", null);
        servlet.execute(request, response, true);
        WritePipeTest.assertPiped(resource);
    }
    
    @Test
    public void testDummyTreeSizeLimit() throws Exception {
        SlingHttpServletRequest request = mockPlumberServletRequest(context.resourceResolver(), "json", dummyTreePath, null, null, null, null, "2");
        servlet.execute(request, response, false);
        assertDummyTree(2);
    }

    @Test
    public void testDummyTreeInfiniteSize() throws Exception {
        SlingHttpServletRequest request = mockPlumberServletRequest(context.resourceResolver(), "json", dummyTreePath, null, null, null, null, "-1");
        servlet.execute(request, response, false);
        assertDummyTree(DUMMYTREE_TEST_SIZE);
    }

    public static SlingHttpServletRequest mockPlumberServletRequest(ResourceResolver resolver,
                                                                    String extension,
                                                                    String path,
                                                                    String pathParam,
                                                                    String bindings,
                                                                    String writer,
                                                                    String dryRun,
                                                                    String size) {
        SlingHttpServletRequest request = mock(SlingHttpServletRequest.class);
        RequestPathInfo pathInfo = mock(RequestPathInfo.class);
        when(pathInfo.getExtension()).thenReturn(extension);
        when(request.getRequestPathInfo()).thenReturn(pathInfo);
        Resource resource = resolver.getResource(path);
        when(request.getResourceResolver()).thenReturn(resolver);
        when(request.getResource()).thenReturn(resource);
        when(request.getParameter(PlumberServlet.PARAM_PATH)).thenReturn(pathParam);
        when(request.getParameter(PlumberServlet.PARAM_BINDINGS)).thenReturn(bindings);
        when(request.getParameter(OutputWriter.PARAM_WRITER)).thenReturn(writer);
        when(request.getParameter(BasePipe.DRYRUN_KEY)).thenReturn(dryRun);
        when(request.getParameter(OutputWriter.PARAM_SIZE)).thenReturn(size);
        return request;
    }

    public static SlingHttpServletResponse mockPlumberServletResponse(StringWriter writer) throws IOException {
        SlingHttpServletResponse response = mock(SlingHttpServletResponse.class);
        PrintWriter printWriter = new PrintWriter(writer);
        when(response.getWriter()).thenReturn(printWriter);
        return response;
    }
}
