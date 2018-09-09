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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.Privilege;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.script.ScriptException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

public class ACLPipe extends BasePipe {
    private static Logger logger = LoggerFactory.getLogger(ACLPipe.class);
    public static final String RESOURCE_TYPE = RT_PREFIX + "acl";
    public static final String PN_USERNAME = "userName";
    public static final String PN_ALLOW = "allow";
    public static final String PN_DENY = "deny";
    public static final String PN_AUTHORIZABLE = "authorizable";
    public static final String PATH_KEY = "path";
    public static final String PRIVILEGES_KEY = "rep:privileges";
    public static final String ACE_GRANT_KEY = "rep:GrantACE";
    public static final String ACE_DENY_KEY = "rep:DenyACE";
    public static final String JCR_PRIVILEGES_INPUT = "jcr:privileges";
    public static final String PRIVILEGES_JSON_KEY = "privileges";

    Session session;
    ResourceResolver resolver;
    UserManager userManager;
    Privilege[] privileges;
    String[] privilegesInput;
    boolean allow;
    boolean deny;
    boolean authorizable;
    Object outputBinding;

    @Override
    public Object getOutputBinding() {
        if (outputBinding != null) {
            return outputBinding;
        }
        return super.getOutputBinding();
    }

    @Override
    public boolean modifiesContent() {
        return allow || deny ;
    }

    /**
     * public constructor
     *
     * @param plumber  plumber instance
     * @param resource configuration resource
     * @throws Exception bad configuration handling
     */
    public ACLPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) throws Exception {
        super(plumber, resource, upperBindings);
        resolver = resource.getResourceResolver();
        session = resolver.adaptTo(Session.class);
        userManager = resolver.adaptTo(UserManager.class);
        if (getConfiguration() != null) {
            ValueMap properties = getConfiguration().adaptTo(ValueMap.class);
            privilegesInput = properties.get(JCR_PRIVILEGES_INPUT, new String[]{});
            allow = properties.get(PN_ALLOW, false);
            deny = properties.get(PN_DENY, false);
            authorizable = properties.get(PN_AUTHORIZABLE, false);
        }
    }

    @Override
    public Iterator<Resource> computeOutput() throws Exception {
        Resource resource = getInput();
        if (resource != null) {
            if (allow || deny) {
                logger.info("Going to changing ACL for the resource at path {}", resource.getPath());
                if (StringUtils.isEmpty(getExpr())) {
                    throw new Exception("expression for the principal or authorizable Id should be provided or provided correctly for privileges to be set");
                }
                Principal principal = getPrincipalFor(getExpr());
                if ( null != privilegesInput && privilegesInput.length == 0 ){
                    // create a privilege set with jcr:all
                    privileges = allow ? AccessControlUtils.privilegesFromNames(session, Privilege.JCR_ALL) : AccessControlUtils.privilegesFromNames(session, Privilege.JCR_READ);
                }
                else {
                    try {
                        privilegesInput = processPrivilegesInput(privilegesInput);
                        privileges = AccessControlUtils.privilegesFromNames(session, privilegesInput);
                    }
                    catch (Exception e) {
                        logger.error("unable to get privileges , either privileges input is wrong either error evaluting bindings for privileges", e);
                    }
                }
                if (!isDryRun()) {
                    if (allow) {
                        logger.info("adding privileges {} for principal {} allow {}",ArrayUtils.toString(privileges), principal.getName(), allow);
                        AccessControlUtils.addAccessControlEntry(session, resource.getPath(), principal, privileges, true);
                    } else if (deny) {
                        logger.info("adding privileges {} for principal {} deny {}",ArrayUtils.toString(privileges), principal.getName(), deny);
                        AccessControlUtils.addAccessControlEntry(session, resource.getPath(), principal, privileges, false);
                    }
                    if (session.hasPendingChanges()) {
                        session.save();
                    }
                    return super.computeOutput();
                }
            }
            bindACLs(resource);
            return super.computeOutput();
        }
        return EMPTY_ITERATOR;
    }

    /**
     * Binds ACLs of the current resource to the bindings
     * If current Resource is an Authorizable and authorizable flag is true then ACLs of the authorizablie on repository is put in bindings
     * @param resource current resource
     */

    protected void bindACLs(Resource resource) {
        try {
            Authorizable auth = checkIsAuthorizableResource(resource);
            if ( null != auth ) {
                //get privileges for an auth on the repository , authorizable flag should be true if resource is an authorizable
                if ( authorizable ) {
                    bindAclsForAuthorizableResource(auth);
                    return;
                }
                throw new Exception("authorizable flag should set to true for" + resource.getPath() + "if resource is an authorizable");
            }
            logger.info("binding acls for resource at path {}", resource.getPath());
            AccessControlList acl = AccessControlUtils.getAccessControlList(session, resource.getPath());
            JackrabbitAccessControlEntry[] entries = (JackrabbitAccessControlEntry[]) acl.getAccessControlEntries();
            JsonArrayBuilder principal_Privileges_Array = Json.createArrayBuilder();
            for (JackrabbitAccessControlEntry entry : entries) {
                JsonObjectBuilder principal_Privileges_Mappings = Json.createObjectBuilder();
                JsonArrayBuilder privileges = Json.createArrayBuilder();
                for ( Privilege privilege : entry.getPrivileges() ){
                    privileges.add(privilege.getName());
                }
                principal_Privileges_Mappings.add(PN_AUTHORIZABLE, entry.getPrincipal().getName());
                principal_Privileges_Mappings.add(PRIVILEGES_JSON_KEY, privileges);
                if (entry.isAllow()) {
                    principal_Privileges_Mappings.add(PN_ALLOW, true);
                } else {
                    principal_Privileges_Mappings.add(PN_DENY, true);
                }
                principal_Privileges_Array.add(principal_Privileges_Mappings);
            }
            outputBinding = JsonUtil.toString(principal_Privileges_Array);
        } catch ( Exception e ) {
            logger.error("unable to bind acls", e);
        }

    }

    /**
     * Binds ACLs of an Authorizable on repository
     * @param authorizable current resource as an authorizable
     */

    protected void bindAclsForAuthorizableResource(Authorizable auth) throws RepositoryException {
        //query for searching in full repository where auth is prinicpal in access control entry.
        logger.info("binding acls for authorizable {} and authID {}", auth.getPath(), auth.getID());
        String query = "/jcr:root//element(*, rep:ACE)[@rep:principalName = '" + auth.getID() + "']";
        Iterator<Resource> resources = resolver.findResources(query, javax.jcr.query.Query.XPATH);
        JsonObjectBuilder authPermisions = Json.createObjectBuilder();
        JsonArrayBuilder allowArray = Json.createArrayBuilder();
        JsonArrayBuilder denyArray = Json.createArrayBuilder();
        resources.forEachRemaining((res) -> {
            String[] privileges = res.adaptTo(ValueMap.class).get(PRIVILEGES_KEY, String[].class);
            JsonArrayBuilder privilegesArray = Json.createArrayBuilder();
            for(String privilege: privileges){
                privilegesArray.add(privilege);
            }
            JsonObjectBuilder ACEObject = Json.createObjectBuilder();
            ACEObject.add(PATH_KEY, res.getParent().getParent().getPath());
            ACEObject.add(PRIVILEGES_JSON_KEY, privilegesArray);
            if (res.getResourceType().equals(ACE_GRANT_KEY) ){
                allowArray.add(ACEObject);
            }
            else if (res.getResourceType().equals(ACE_DENY_KEY)) {
                denyArray.add(ACEObject);
            }
        });
        authPermisions.add(PN_AUTHORIZABLE, auth.getID());
        authPermisions.add(PN_ALLOW, allowArray);
        authPermisions.add(PN_DENY, denyArray);
        outputBinding = JsonUtil.toString(authPermisions);
    }

    protected Authorizable checkIsAuthorizableResource(Resource resource) {
      return resource.adaptTo(Authorizable.class);
    }

    /**
     * get Principal for principal name set as an expression in the pipe
     * @param prinicipalName for which the principal has to be found
     */

    protected Principal getPrincipalFor(String prinicipalName) {
        Principal principal = null;
        try {
            if (StringUtils.isNotBlank(prinicipalName)) {
                logger.debug("try to find principalId {}", prinicipalName);
                JackrabbitSession jackrabbitSession = ( JackrabbitSession )session;
                PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
                principal = principalManager.getPrincipal(prinicipalName);
            }
        } catch (Exception e){
            logger.error("unable to get principal for principalName {} ", prinicipalName, e);
        }
        return principal;
    }

    private String[] processPrivilegesInput(String[] privilegesInput) throws ScriptException {
        String expr = bindings.instantiateExpression(ArrayUtils.toString(privilegesInput));
        if ( expr.indexOf("[") > -1 && expr.indexOf("]") > -1) {
            return Arrays.stream(expr.substring(expr.indexOf("[") + 1, expr.indexOf("]")).split(",")).map((s) -> s.trim()).toArray(size->new String[size]);
        }
        return privilegesInput;
    }
}