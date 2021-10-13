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
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import java.util.Collections;
import java.util.Iterator;

/**
 * pipe that outputs an authorizable resource based on the id set in expr
 */
public class AuthorizablePipe extends BasePipe {
    private static Logger logger = LoggerFactory.getLogger(AuthorizablePipe.class);
    public static final String RESOURCE_TYPE = RT_PREFIX + "authorizable";
    public static final String PN_CREATEGROUP = "createGroup";
    public static final String PN_ADDTOGROUP = "addToGroup";
    public static final String PN_ADDMEMBERS = "addMembers";
    public static final String PN_BINDMEMBERS = "bindMembers";

    UserManager userManager;
    boolean createGroup;
    boolean bindMembers;
    String addToGroup;
    String addMembers;
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
        return createGroup || StringUtils.isNotBlank(addToGroup) || StringUtils.isNotBlank(addMembers);
    }

    /**
     * public constructor
     * @param plumber plumber instance
     * @param resource configuration resource
     * @param upperBindings bindings coming from super pipe
     */
    public AuthorizablePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        userManager = resolver.adaptTo(UserManager.class);
        if (getConfiguration() != null) {
            ValueMap properties = getConfiguration().getValueMap();
            createGroup = properties.get(PN_CREATEGROUP, false);
            bindMembers = properties.get(PN_BINDMEMBERS, false);
            addToGroup = properties.get(PN_ADDTOGROUP, String.class);
            addMembers = properties.get(PN_ADDMEMBERS, String.class);
        }
    }

    @Override
    public Iterator<Resource> computeOutput() {
        try {
            Authorizable auth = getAuthorizable();
            if (auth != null) {
                logger.debug("Retrieved authorizable {}", auth.getID());
                if (StringUtils.isNotBlank(addToGroup)) {
                    addAuthorizableToGroup(auth);
                }
                if (StringUtils.isNotBlank(addMembers)) {
                    addMembersToAuthorizable(auth);
                }
                if (bindMembers) {
                    bindMembersToAuthorizable(auth);
                }
                Resource resource = resolver.getResource(auth.getPath());
                return Collections.singleton(resource).iterator();
            }
        } catch (RepositoryException e) {
            throw new IllegalStateException(e);
        }
        return EMPTY_ITERATOR;
    }

    /**
     * Returns the authorizable configured by its expression, creating it if
     * not present and if <code>createGroup</code> is set to true, or, if
     * no expression, tries to resolve getInput() as an authorizable
     * @return corresponding authorizable
     * @throws RepositoryException can happen with any JCR operation
     */
    Authorizable getAuthorizable() throws RepositoryException {
        Authorizable auth = null;
        String authId = getExpr();
        if (StringUtils.isNotBlank(authId)) {
            logger.debug("try to find authorizable {}", authId);
            auth = userManager.getAuthorizable(authId);
            if (auth == null && createGroup) {
                logger.info("authorizable {} does not exist, and createGroup flag is set, creating it", authId);
                if (! isDryRun()) {
                    auth = userManager.createGroup(authId);
                }
            }
        } else {
            Resource resource = getInput();
            if (resource != null) {
                auth = userManager.getAuthorizableByPath(resource.getPath());
            }
        }
        return auth;
    }

    /**
     * Add current authorizable to configured addToGroup expression (should resolve as a group id)
     * @param auth authorizable to add to the group
     */
    void addAuthorizableToGroup(Authorizable auth){
        try {
            //if addToGroup is set to true, we try to find the corresponding
            //group and to add current auth to it as a member
            String groupId = bindings.instantiateExpression(addToGroup);
            Authorizable groupAuth = userManager.getAuthorizable(groupId);
            if (groupAuth != null && groupAuth.isGroup()) {
                logger.info("adding {} to {}", auth.getID(), groupId);
                if (! isDryRun()) {
                    ((Group) groupAuth).addMember(auth);
                }
            }
        } catch (Exception e){
            logger.error("Unable to add current authorizable to group {}", addToGroup, e);
        }
    }

    /**
     * Add to current authorizable (that should be a group) the configured members in addMembers expression
     * @param auth group to which members should be added
     */
    void addMembersToAuthorizable(Authorizable auth) {
        try {
            if (auth.isGroup()) {
                Group group = (Group)auth;
                String uids = bindings.instantiateExpression(addMembers);
                JsonArray array = JsonUtil.parseArray(uids);
                for (int index = 0; index < array.size(); index ++){
                    String uid = array.getString(index);
                    Authorizable member = userManager.getAuthorizable(uid);
                    if (member != null) {
                        logger.info("adding {} to group {}", member.getID(), group.getID());
                        if (!isDryRun()) {
                            group.addMember(member);
                        }
                    } else {
                        logger.error("computed uid {} doesn't exist, doing nothing", uid);
                    }
                }
            } else {
                logger.error("{} is not a group, can't add members", auth.getID());
            }
        } catch (Exception e){
            logger.error("unable to add members {}", addMembers, e);
        }
    }

    /**
     * add current group's members to the bindings
     * @param auth group whose members should be bound in the pipe bindings
     */
    void bindMembersToAuthorizable(Authorizable auth){
        try {
            if (auth.isGroup()){
                Group group = (Group)auth;
                Iterator<Authorizable> memberIterator = group.getMembers();
                JsonArrayBuilder array = Json.createArrayBuilder();
                while (memberIterator.hasNext()){
                    array.add(memberIterator.next().getID());
                }
                outputBinding = JsonUtil.toString(array);
            } else {
                logger.error("{} is not a group, unable to bind members", auth.getID());
            }
        } catch (Exception e){
            logger.error("unable to bind members");
        }
    }
}
