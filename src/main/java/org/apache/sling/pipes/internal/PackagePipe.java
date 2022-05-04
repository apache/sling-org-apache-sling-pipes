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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.fs.api.PathFilterSet;
import org.apache.jackrabbit.vault.fs.config.DefaultWorkspaceFilter;
import org.apache.jackrabbit.vault.packaging.JcrPackage;
import org.apache.jackrabbit.vault.packaging.JcrPackageDefinition;
import org.apache.jackrabbit.vault.packaging.JcrPackageManager;
import org.apache.jackrabbit.vault.packaging.PackageException;
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

    boolean filterCollectionMode;

    /**
     * Pipe Constructor
     *
     * @param plumber  plumber
     * @param resource configuration resource
     * @param upperBindings super pipe's bindings
     */
    public PackagePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        assemble = properties.get(PN_ASSEMBLE, true);
        checkExistence = properties.get(PN_CHECKEXISTENCE, true);
        filterCollectionMode = properties.get(PN_FILTERCOLLECTIONMODE, false);
    }

    @Override
    public boolean modifiesContent() {
        return true;
    }

    private Iterator<Resource> collectFilter() throws RepositoryException {
        Resource filterResource = getInput();
        if (filterResource != null || !checkExistence) {
            if (jcrPackage == null) {
                throw new IllegalArgumentException("Something went wrong while initiating the package");
            }
            if (filters == null) {
                filters = new DefaultWorkspaceFilter();
            }
            //we take as a filter either computed resource, either configured path, as if resource,
            //is null, check existence has been configured to be false
            String filter = filterResource != null ? filterResource.getPath() : getComputedPath();
            filters.add(new PathFilterSet(filter));
            JcrPackageDefinition definition = jcrPackage.getDefinition();
            if (definition == null) {
                LOGGER.warn("package {} definition is null", jcrPackage);
            } else {
                definition.setFilter(filters, true);
                return IteratorUtils.singletonIterator(getInput());
            }
        }
        return EMPTY_ITERATOR;
    }

    @Override
    protected Iterator<Resource> computeOutput() {
        try {
            init();
            return filterCollectionMode ? collectFilter() : EMPTY_ITERATOR;
        } catch (RepositoryException | IOException e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * computes configured package based on expression configuration (either existing or creating it)
     * @throws IOException problem with binary
     * @throws RepositoryException problem with package persistence
     * @throws IOException problem with package build
     */
    protected void init() throws IOException, RepositoryException {
        if (jcrPackage == null){
            String packagePath = getExpr();
            Session session = resolver.adaptTo(Session.class);
            if (StringUtils.isNotBlank(packagePath) && session != null) {
                JcrPackageManager mgr = PackagingService.getPackageManager(session);
                Node pkgNode = session.getNode(packagePath);
                if (pkgNode != null) {
                    jcrPackage = mgr.open(pkgNode);
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
    public void after() {
        super.after();
        if (assemble) {
            try {
                JcrPackageManager mgr = PackagingService.getPackageManager(resolver.adaptTo(Session.class));
                mgr.assemble(jcrPackage, null);
            } catch (PackageException | RepositoryException | IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
