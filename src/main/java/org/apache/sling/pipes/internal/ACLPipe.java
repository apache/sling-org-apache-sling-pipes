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
import java.security.Principal;
import java.util.Arrays;
import java.util.Iterator;

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
    UserManager userManager;
    Privilege[] privileges;
    String[] privilegesInput;
    boolean allow;
    boolean deny;
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
     * @param upperBindings already set binding we want to initiate our pipe with
     */
    public ACLPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        session = resolver.adaptTo(Session.class);
        userManager = resolver.adaptTo(UserManager.class);
        privilegesInput = properties.get(JCR_PRIVILEGES_INPUT, new String[]{});
        allow = properties.get(PN_ALLOW, false);
        deny = properties.get(PN_DENY, false);
    }

    Iterator<Resource> computeAllowDenyOutput(Resource resource) throws RepositoryException {
        logger.debug("Going to changing ACL for the resource at path {}", resource.getPath());
        if (StringUtils.isEmpty(getExpr())) {
            throw new IllegalArgumentException("expression for the principal or authorizable Id should be provided or provided correctly for privileges to be set");
        }
        Principal principal = getPrincipalFor(getExpr());
        if (ArrayUtils.isEmpty(privilegesInput)) {
            // create a privilege set with jcr:all if allow or jcr:read if deny
            privileges = allow ? AccessControlUtils.privilegesFromNames(session, Privilege.JCR_ALL) : AccessControlUtils.privilegesFromNames(session, Privilege.JCR_READ);
        } else {
            privilegesInput = processPrivilegesInput(privilegesInput);
            privileges = AccessControlUtils.privilegesFromNames(session, privilegesInput);
        }
        addAccessControlEntry(resource, principal);
        return super.computeOutput();
    }

    @Override
    public Iterator<Resource> computeOutput()  {
        Resource resource = getInput();
        try {
            if (resource != null) {
                if (allow || deny) {
                    return computeAllowDenyOutput(resource);
                } else {
                    bindACLs(resource);
                    return super.computeOutput();
                }
            }
        } catch (RepositoryException e) {
            logger.error("unable to properly treat ACLs, returning empty iterator", e);
        }
        return EMPTY_ITERATOR;
    }

    /**
     * Binds ACLs of the current resource to the bindings
     * If current Resource is an Authorizable and authorizable flag is true then ACLs of the authorizablie on repository is put in bindings
     * @param resource current resource
     */

    void bindACLs(Resource resource) {
        try {
            Authorizable auth = checkIsAuthorizableResource(resource);
            if ( null != auth ) {
                //get privileges for an auth on the repository , authorizable flag should be true if resource is an authorizable
                bindAclsForAuthorizableResource(auth);
                return;
            }
            logger.info("binding acls for resource at path {}", resource.getPath());
            AccessControlList acl = AccessControlUtils.getAccessControlList(session, resource.getPath());
            JackrabbitAccessControlEntry[] entries = (JackrabbitAccessControlEntry[]) acl.getAccessControlEntries();
            JsonArrayBuilder principalPrivilegesArray = Json.createArrayBuilder();
            for (JackrabbitAccessControlEntry entry : entries) {
                JsonObjectBuilder principalPrivilegesMappings = Json.createObjectBuilder();
                JsonArrayBuilder privilegeSet = Json.createArrayBuilder();
                for ( Privilege privilege : entry.getPrivileges() ){
                    privilegeSet.add(privilege.getName());
                }
                principalPrivilegesMappings.add(PN_AUTHORIZABLE, entry.getPrincipal().getName());
                principalPrivilegesMappings.add(PRIVILEGES_JSON_KEY, privilegeSet);
                if (entry.isAllow()) {
                    principalPrivilegesMappings.add(PN_ALLOW, true);
                } else {
                    principalPrivilegesMappings.add(PN_DENY, true);
                }
                principalPrivilegesArray.add(principalPrivilegesMappings);
            }
            outputBinding = JsonUtil.toString(principalPrivilegesArray);
        } catch ( Exception e ) {
            outputBinding = JsonUtil.toString(Json.createObjectBuilder());
            logger.error("unable to bind acls", e);
        }

    }

    /**
     * Binds ACLs of an Authorizable on repository
     * @param auth current resource as an authorizable
     * @throws RepositoryException in case something goes wrong while executing xpath query
     */

    void bindAclsForAuthorizableResource(Authorizable auth) throws RepositoryException {
        //query for searching in full repository where auth is prinicpal in access control entry.
        logger.info("binding acls for authorizable {} and authID {}", auth.getPath(), auth.getID());
        String query = "/jcr:root//element(*, rep:ACE)[@rep:principalName = '" + auth.getID() + "']";
        Iterator<Resource> resources = resolver.findResources(query, javax.jcr.query.Query.XPATH);
        JsonObjectBuilder authPermisions = Json.createObjectBuilder();
        JsonArrayBuilder allowArray = Json.createArrayBuilder();
        JsonArrayBuilder denyArray = Json.createArrayBuilder();
        resources.forEachRemaining(res -> {
            String[] privilegeSet = res.adaptTo(ValueMap.class).get(PRIVILEGES_KEY, String[].class);
            JsonArrayBuilder privilegesArray = Json.createArrayBuilder();
            for(String privilege: privilegeSet){
                privilegesArray.add(privilege);
            }
            JsonObjectBuilder aceObj = Json.createObjectBuilder();
            aceObj.add(PATH_KEY, res.getParent().getParent().getPath());
            aceObj.add(PRIVILEGES_JSON_KEY, privilegesArray);
            if (res.getResourceType().equals(ACE_GRANT_KEY) ){
                allowArray.add(aceObj);
            }
            else if (res.getResourceType().equals(ACE_DENY_KEY)) {
                denyArray.add(aceObj);
            }
        });
        authPermisions.add(PN_AUTHORIZABLE, auth.getID());
        authPermisions.add(PN_ALLOW, allowArray);
        authPermisions.add(PN_DENY, denyArray);
        outputBinding = JsonUtil.toString(authPermisions);
    }

    Authorizable checkIsAuthorizableResource(Resource resource) {
        return resource.adaptTo(Authorizable.class);
    }

    /**
     * get Principal for principal name set as an expression in the pipe
     * @param prinicipalName for which the principal has to be found
     * @return Principal for the principalName
     */

    Principal getPrincipalFor(String prinicipalName) {
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

    private void addAccessControlEntry(Resource resource, Principal principal) throws RepositoryException {
        if (logger.isInfoEnabled()) {
            logger.info("adding privileges {} for principal {} allow {} deny {} with dryRun {} ",
                ArrayUtils.toString(privileges), principal.getName(), allow, deny, isDryRun());
        }
        if (!isDryRun()) {
            if (allow) {
                AccessControlUtils.addAccessControlEntry(session, resource.getPath(), principal, this.privileges, true);
            } else if (deny) {
                AccessControlUtils.addAccessControlEntry(session, resource.getPath(), principal, this.privileges, false);
            }
        }
    }

    private String[] processPrivilegesInput(String[] privilegesInput) {
        String expr = bindings.instantiateExpression(ArrayUtils.toString(privilegesInput));
        if ( expr.indexOf("[") > -1 && expr.indexOf("]") > -1) {
            return Arrays.stream(expr.substring(expr.indexOf("[") + 1, expr.indexOf("]")).split(","))
                .map(String::trim).toArray(size->new String[size]);
        }
        return privilegesInput;
    }
}