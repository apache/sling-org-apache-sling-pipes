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

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.fs.api.PathFilterSet;
import org.apache.jackrabbit.vault.fs.config.DefaultWorkspaceFilter;
import org.apache.jackrabbit.vault.packaging.JcrPackage;
import org.apache.jackrabbit.vault.packaging.JcrPackageManager;
import org.apache.jackrabbit.vault.packaging.PackagingService;
import org.apache.jackrabbit.vault.util.Text;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.Iterator;

/**
 * Package pipe, creates or read vault package
 */
public class PackagePipe extends BasePipe {
    private static final Logger LOGGER = LoggerFactory.getLogger(PackagePipe.class);

    public static final String RESOURCE_TYPE = "slingPipes/package";

    public static final String PN_FILTERCOLLECTIONMODE = "filterCollectionMode";

    public static final String PN_ASSEMBLE = "assemble";

    public static final String PN_CHECKEXISTENCE = "checkExistence";

    DefaultWorkspaceFilter filters;

    JcrPackage jcrPackage;

    boolean assemble;

    boolean checkExistence;

    /**
     * Pipe Constructor
     *
     * @param plumber  plumber
     * @param resource configuration resource
     *
     * @throws Exception in case configuration is not working
     */
    public PackagePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) throws Exception {
        super(plumber, resource, upperBindings);
        assemble = properties.get(PN_ASSEMBLE, true);
        checkExistence = properties.get(PN_CHECKEXISTENCE, true);
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    @Override
    protected Iterator<Resource> computeOutput() throws Exception {
        Iterator<Resource> output = EMPTY_ITERATOR;
        init();
        if (properties.get(PN_FILTERCOLLECTIONMODE, false)){
            Resource filterResource = getInput();
            if (filterResource != null || !checkExistence){
                if (filters == null){
                    filters = new DefaultWorkspaceFilter();
                }
                //we take as a filter either computed resource, either configured path, as if resource,
                //is null, check existence has been configured to be false
                String filter = filterResource != null ? filterResource.getPath() : getComputedPath();
                filters.add(new PathFilterSet(filter));
                jcrPackage.getDefinition().setFilter(filters, true);
                output = IteratorUtils.singletonIterator(getInput());
            }
        }
        return output;
    }


    /**
     * computes configured package based on expression configuration (either existing or creating it)
     * @throws IOException problem with binary
     * @throws RepositoryException problem with package persistence
     */
    protected void init() throws IOException, RepositoryException, ScriptException {
        if (jcrPackage == null){
            String packagePath = getExpr();
            if (StringUtils.isNotBlank(packagePath)) {
                JcrPackageManager mgr = PackagingService.getPackageManager(resolver.adaptTo(Session.class));
                Resource packageResource = resolver.getResource(packagePath);
                if (packageResource != null) {
                    jcrPackage = mgr.open(packageResource.adaptTo(Node.class));
                } else {
                    String parent = Text.getRelativeParent(packagePath, 1);
                    Resource folderResource = resolver.getResource(parent);
                    if (folderResource == null) {
                        LOGGER.error("folder of configured path should exists");
                    } else {
                        String name = Text.getName(packagePath);
                        jcrPackage = mgr.create(folderResource.adaptTo(Node.class), name);
                    }
                }
            } else {
                LOGGER.error("expression should not be blank as it's supposed to hold package path");
            }
        }
    }

    @Override
    public void after() throws Exception {
        super.after();
        if (assemble) {
            JcrPackageManager mgr = PackagingService.getPackageManager(resolver.adaptTo(Session.class));
            mgr.assemble(jcrPackage, null);
        }
    }
}
