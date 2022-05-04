/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.pipes.it;

import javax.inject.Inject;
import javax.servlet.Servlet;

import org.apache.commons.codec.binary.Base64;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.auth.core.AuthenticationSupport;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.resource.presence.ResourcePresence;
import org.apache.sling.testing.paxexam.SlingVersionResolver;
import org.apache.sling.testing.paxexam.TestSupport;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.util.Filter;

import static org.apache.sling.testing.paxexam.SlingOptions.slingCaconfig;
import static org.apache.sling.testing.paxexam.SlingOptions.slingCommonsHtml;
import static org.apache.sling.testing.paxexam.SlingOptions.slingDistribution;
import static org.apache.sling.testing.paxexam.SlingOptions.slingEvent;
import static org.apache.sling.testing.paxexam.SlingOptions.slingQuery;
import static org.apache.sling.testing.paxexam.SlingOptions.slingQuickstartOakTar;
import static org.apache.sling.testing.paxexam.SlingOptions.slingResourcePresence;
import static org.apache.sling.testing.paxexam.SlingOptions.slingScriptingSightly;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

public abstract class PipesTestSupport extends TestSupport {

    protected static final String NN_TEST = "test";

    protected static final String ADMIN_CREDENTIALS = "admin:admin";

    @Inject
    protected Plumber plumber;

    @Inject
    @Filter("(sling.servlet.resourceTypes=slingPipes/plumber)")
    private Servlet plumberServlet;

    @Inject
    protected AuthenticationSupport authenticationSupport;

    @Inject
    private ResourceResolverFactory resourceResolverFactory;

    final protected int httpPort = findFreePort();

    @Inject
    @Filter(value = "(path=/etc/pipes-it/fruit-list)")
    private ResourcePresence listPresence;

    @Configuration
    public Option[] configuration() {
        SlingVersionResolver versionResolver = new SlingVersionResolver();
        return options(
            baseConfiguration(),
            quickstart(),
            // Sling Pipes
            testBundle("bundle.filename"),
            factoryConfiguration("org.apache.sling.resource.presence.internal.ResourcePresenter")
                .put("path", "/etc/pipes-it/fruit-list")
                .asOption(),
            mavenBundle().groupId("org.apache.geronimo.bundles").artifactId("commons-httpclient").version(versionResolver),
            // testing
            slingResourcePresence(),
            slingCaconfig(),
            newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                .put("whitelist.bundles.regexp", "^PAXEXAM.*$")
                .asOption(),
            mavenBundle().groupId("com.google.code.gson").artifactId("gson").versionAsInProject(),
            mavenBundle().groupId("org.jsoup").artifactId("jsoup").versionAsInProject(),
            mavenBundle().groupId("org.apache.servicemix.bundles").artifactId("org.apache.servicemix.bundles.hamcrest").versionAsInProject(),
            mavenBundle().groupId("org.apache.commons").artifactId("commons-collections4").versionAsInProject(),
            mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.caconfig.api").versionAsInProject(),
            mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.caconfig.impl").versionAsInProject(),
            junitBundles()
        );
    }

    protected String basicAuthorizationHeader(final String credentials) {
        return "Basic ".concat(new String(Base64.encodeBase64(credentials.getBytes())));
    }

    protected void mkdir(ResourceResolver resolver, String path) throws Exception {
        plumber.newPipe(resolver).mkdir(path).run();
    }

    protected Option quickstart() {
        final String workingDirectory = workingDirectory();
        return composite(
            slingQuickstartOakTar(workingDirectory, httpPort),
            slingEvent(),
            slingDistribution(),
            slingQuery(),
            slingCommonsHtml(),
            slingScriptingSightly()
        );
    }

    @ProbeBuilder
    public TestProbeBuilder probeConfiguration(final TestProbeBuilder testProbeBuilder) {
        testProbeBuilder.setHeader("Sling-Initial-Content","initial-content"
        );
        return testProbeBuilder;
    }

    ResourceResolver resolver() throws LoginException {
        return resourceResolverFactory.getAdministrativeResourceResolver(null);
    }

}
