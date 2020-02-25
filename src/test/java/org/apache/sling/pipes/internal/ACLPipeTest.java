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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import javax.jcr.Session;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.Privilege;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import java.security.Principal;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ACLPipeTest extends AbstractPipeTest {

    @Rule
    public SlingContext oak = new SlingContext(ResourceResolverType.JCR_OAK);

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        oak.load().json("/acl.json", PATH_PIPE);
        oak.load().json("/initial-content/content/fruits.json", PATH_FRUITS);
    }

    @Test
    public void testAllow() throws Exception{
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + "/allow"));
        Iterator<Resource> outputs = pipe.getOutput();
        JackrabbitAccessControlEntry[] entries = retrieveACLsSetOnResourceWithPipe(pipe);
        assertNotNull(entries);
        assertEquals("There should be only one entry", 1, entries.length);
        assertEquals("Entry prinicipal should be anonymous", "anonymous", entries[0].getPrincipal().getName());
        assertEquals("Entry should be allow or rep:GrantACE", true, entries[0].isAllow());
        assertEquals("Entry privileges should be jcr:read", "jcr:read", entries[0].getPrivileges()[0].getName());
    }

    @Test
    public void testAllowDefault() throws Exception{
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + "/allowDefault"));
        Iterator<Resource> outputs = pipe.getOutput();
        JackrabbitAccessControlEntry[] entries = retrieveACLsSetOnResourceWithPipe(pipe);assertNotNull(entries);
        assertEquals("There should be only one entry", 1, entries.length);
        assertEquals("Entry prinicipal should be anonymous", "anonymous", entries[0].getPrincipal().getName());
        assertEquals("Entry should be allow or rep:GrantACE", true, entries[0].isAllow());
        assertEquals("Entry privileges should be jcr:all by default", "jcr:all", entries[0].getPrivileges()[0].getName());
    }

    @Test
    public void testDeny() throws Exception{
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + "/deny"));
        Iterator<Resource> outputs = pipe.getOutput();
        JackrabbitAccessControlEntry[] entries = retrieveACLsSetOnResourceWithPipe(pipe);
        assertNotNull(entries);
        assertEquals("There should be only one entry", 1, entries.length);
        assertEquals("Entry prinicipal should be anonymous", "anonymous", entries[0].getPrincipal().getName());
        assertEquals("Entry should be deny or rep:DenyACE", false, entries[0].isAllow());
        assertEquals("Entry privileges should be jcr:write", "jcr:write", entries[0].getPrivileges()[0].getName());
    }

    @Test
    public void testDenyDefault() throws Exception{
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + "/denyDefault"));
        Iterator<Resource> outputs = pipe.getOutput();
        JackrabbitAccessControlEntry[] entries = retrieveACLsSetOnResourceWithPipe(pipe);
        assertNotNull(entries);
        assertEquals("There should be only one entry", 1, entries.length);
        assertEquals("Entry prinicipal should be anonymous", "anonymous", entries[0].getPrincipal().getName());
        assertEquals("Entry should be deny or rep:DenyACE", false, entries[0].isAllow());
        assertEquals("Entry privileges should be jcr:read by default", "jcr:read", entries[0].getPrivileges()[0].getName());
    }

    @Test
    public void testACLs() throws Exception{
        setACLsOnResourceWithPath(PATH_FRUITS);
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + "/acl"));
        pipe.getOutput();
        Object bindings = pipe.getOutputBinding();
        assertNotNull(bindings);
        JsonStructure json = JsonUtil.parse((String)bindings);
        assertEquals(json.getValueType().name(), "ARRAY");
        JsonArray jsonValues = JsonUtil.parseArray((String)bindings);
        assertEquals("size should be one", 1, jsonValues.size());
        assertEquals("values should be equal", jsonValues.getJsonObject(0).getString("authorizable"), "anonymous");
        assertEquals("values should be equal", JsonUtil.toString(jsonValues.getJsonObject(0).get("privileges")), "[\"jcr:read\"]");
    }

    @Ignore
    @Test
    /*
    Ignoring the test for now since Oak mocking doesn't support resolver.adapt(Authorizable.class)
     */
    public void testACLsWithResourceAsAuthorizable() throws Exception{
        setACLsOnResourceWithPath(PATH_FRUITS);
        Pipe pipe = plumber.getPipe(oak.resourceResolver().getResource(PATH_PIPE + "/aclAuthorizable"));
        pipe.getOutput();
        Object bindings = pipe.getOutputBinding();
        JsonStructure json = JsonUtil.parse((String)bindings);
        assertEquals(json.getValueType().name(), "OBJECT");
        JsonObject jsonValues = JsonUtil.parseObject((String)bindings);
        assertEquals("size should be one", 1, jsonValues.size());
        assertEquals("values should be equal",  jsonValues.getJsonArray("allow").getJsonObject(0).getString("path"),"/content/fruits");
        assertEquals("values should be equal", JsonUtil.toString(jsonValues.getJsonArray("allow").getJsonObject(0).getJsonArray("privileges")), "[\"jcr:read\"]");}

    private void setACLsOnResourceWithPath(String path) throws Exception{
        Session session = oak.resourceResolver().adaptTo(Session.class);
        JackrabbitSession jackrabbitSession = ( JackrabbitSession ) session;
        PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
        Principal principal = principalManager.getPrincipal("anonymous");
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(session, Privilege.JCR_READ);
        AccessControlUtils.addAccessControlEntry(oak.resourceResolver().adaptTo(Session.class), path, principal, privileges, true);
    }

    private JackrabbitAccessControlEntry[] retrieveACLsSetOnResourceWithPipe(Pipe pipe) throws Exception{
        //retrieve acls on resource after setting
        AccessControlList acl = AccessControlUtils.getAccessControlList(oak.resourceResolver().adaptTo(Session.class), pipe.getInput().getPath());
        JackrabbitAccessControlEntry[] entries = (JackrabbitAccessControlEntry[]) acl.getAccessControlEntries();
        return entries;
    }
}
