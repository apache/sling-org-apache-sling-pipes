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
import static org.junit.Assert.assertTrue;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.jcr.Session;
import java.util.Iterator;
import java.util.List;

/**
 * testing moving nodes & properties
 */
public class MovePipeTest extends AbstractPipeTest {

    static final String MOVENODE_PIPE = "/moveNode";
    static final String MOVENODEORDER_PIPE = "/moveNodeOrder";
    static final String MOVEPROPERTY_PIPE = "/moveProperty";
    static final String APPLE_NODE_PATH = "/apple";
    static final String BANANA_NODE_PATH = "/banana";
    static final String MOVED_NODE_PATH = "/granny";
    static final String MOVED_PROPERTY_PATH = "/fruitsIndex";

    @Rule
    public SlingContext oak = new SlingContext(ResourceResolverType.JCR_OAK);

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        oak.load().json("/move.json", PATH_PIPE);
        oak.load().json("/initial-content/content/fruits.json", PATH_FRUITS);
    }

    @Test
    public void modifiesContent() {
        assertTrue(plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + MOVEPROPERTY_PIPE)).modifiesContent());
    }

    @Test
    public void testMoveNode() throws Exception {
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + MOVENODE_PIPE));
        Iterator<Resource> output = pipe.getOutput();
        assertTrue(output.hasNext());
        output.next();
        Session session = oak.resourceResolver().adaptTo(Session.class);
        session.save();
        assertTrue("new node path should exists", session.nodeExists(PATH_FRUITS + MOVED_NODE_PATH));
    }

    @Test
    public void testMoveNodeWithOverwrite() throws Exception {
        String bananaPath = PATH_FRUITS + BANANA_NODE_PATH;
        String applePath = PATH_FRUITS + APPLE_NODE_PATH;
        ExecutionResult result = plumber.newPipe(oak.resourceResolver())
            .echo(applePath)
            .mv(bananaPath).run();
        assertEquals("there should be no overwrite if not specified", 0, result.size());
        result = plumber.newPipe(oak.resourceResolver())
            .echo(applePath)
            .mv(bananaPath).with("overwriteTarget", true).run();
        assertEquals("there should be no overwrite if not specified", 1, result.size());
        Session session = oak.resourceResolver().adaptTo(Session.class);
        assertTrue("target node path should exist", session.nodeExists(bananaPath));
        Assert.assertFalse("source node path should have gone", session.nodeExists(applePath));
    }

    @Test
    public void testMoveNodeWithOrdering() throws Exception {
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + MOVENODEORDER_PIPE));
        Iterator<Resource> output = pipe.getOutput();
        assertTrue(output.hasNext());
        Resource resource = output.next();
        Resource parent = resource.getParent();
        List<Resource> allFruits = IteratorUtils.toList(parent.listChildren());
        Session session = oak.resourceResolver().adaptTo(Session.class);
        session.save();
        assertTrue("target node path should exist", session.nodeExists(PATH_FRUITS + APPLE_NODE_PATH));
        assertTrue("source node path also should exist", session.nodeExists(resource.getPath()));
        Assert.assertEquals("banana should be at first position", allFruits.get(0).getName(), resource.getName());
        Assert.assertEquals("apple should be at first position", allFruits.get(1).getName(), "apple");
    }

    @Test
    public void testMoveProperty() throws Exception {
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + MOVEPROPERTY_PIPE));
        Iterator<Resource> output = pipe.getOutput();
        assertTrue(output.hasNext());
        output.next();
        Session session = oak.resourceResolver().adaptTo(Session.class);
        session.save();
        assertTrue("new property path should exists", session.propertyExists(PATH_FRUITS + MOVED_PROPERTY_PATH));
        Assert.assertFalse("old property path should not", session.propertyExists(PATH_FRUITS + PN_INDEX));
    }

    @Test
    public void testRelativeMove() throws Exception {
        ExecutionResult results = execute(oak.resourceResolver(), "mkdir /content/foo | mv bar");
        assertEquals("there should be 1 result", 1 , results.size());
        assertEquals(results.getCurrentPathSet().iterator().next(), "/content/bar");
    }
}
