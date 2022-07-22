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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingConstants;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.request.RequestParameter;
import org.apache.sling.api.resource.AbstractResourceVisitor;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.caconfig.spi.ConfigurationMetadataProvider;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.Distributor;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.pipes.AbstractInputStreamPipe;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.PipeExecutor;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.PlumberMXBean;
import org.apache.sling.pipes.internal.bindings.ConfigurationMap;
import org.apache.sling.pipes.internal.bindings.JxltEngine;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.script.Bindings;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.security.AccessControlException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;
import static org.apache.sling.pipes.BasePipe.PN_STATUS;
import static org.apache.sling.pipes.BasePipe.PN_STATUS_MODIFIED;
import static org.apache.sling.pipes.BasePipe.SLASH;
import static org.apache.sling.pipes.BasePipe.STATUS_FINISHED;
import static org.apache.sling.pipes.BasePipe.STATUS_STARTED;

/**
 * implements plumber interface, registers default pipes, and provides execution facilities
 */
@Component(service = {Plumber.class, JobConsumer.class, PlumberMXBean.class, Runnable.class}, property = {
        JobConsumer.PROPERTY_TOPICS +"="+PlumberImpl.SLING_EVENT_TOPIC
})
@Designate(ocd = PlumberImpl.Configuration.class)
public class PlumberImpl implements Plumber, JobConsumer, PlumberMXBean, Runnable {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    public static final int DEFAULT_BUFFER_SIZE = 1000;

    static final String PN_MONITORED = "monitored";
    static final String MONITORED_PIPES_QUERY = String.format("//element(*,nt:base)[@sling:resourceType='%s' and @%s]", ContainerPipe.RESOURCE_TYPE, PN_MONITORED);

    static final String MBEAN_NAME_FORMAT = "org.apache.sling.pipes:name=%s";

    static final String PARAM_BINDINGS = "bindings";

    static final String PARAM_FILE = "pipes_inputFile";

    static final String PERMISSION_EXECUTION = "/system/sling/permissions/pipes/exec";

    static final String JCR_LAST_MODIFIED_BY = JCR_LASTMODIFIED + "By";

    static final String JCR_LAST_MODIFIED_BY_PIPE = JCR_LAST_MODIFIED_BY + "Pipe";

    public static final String PIPES_REPOSITORY_PATH = "/var/pipes";

    @ObjectClassDefinition(name="Apache Sling Pipes : Plumber configuration")
    public @interface Configuration {
        @AttributeDefinition(description="Number of iterations after which plumber should saves a pipe execution")
        int bufferSize() default PlumberImpl.DEFAULT_BUFFER_SIZE;

        @AttributeDefinition(description="Number of milliseconds of sleep after each persistence")
        long sleep() default 0L;

        @AttributeDefinition(description="Name of service user, with appropriate rights, that will be used for async execution")
        String serviceUser();

        @AttributeDefinition(description="Path of the permission resource for executing pipes")
        String executionPermissionResource() default PERMISSION_EXECUTION;

        @AttributeDefinition(description="Users allowed to register async pipes")
        String[] authorizedUsers() default  {"admin"};

        @AttributeDefinition(description = "Paths to search for references in")
        String[] referencesPaths() default {};

        @AttributeDefinition(description = "max age (in days) of automatically generated pipe persistence")
        int maxAge() default 31;

        @AttributeDefinition(description = "should add pipe path to updated properties")
        @SuppressWarnings("squid:S100") // osgi convention
        boolean mark_pipe_path() default false;

        @AttributeDefinition(description = "should allow additional scripts feature for any pipe")
        boolean allow_additional_scripts() default true;

        @AttributeDefinition(description = "schedule of purge process")
        @SuppressWarnings("squid:S100") // osgi convention
        String scheduler_expression() default "0 0 12 */7 * ?";
    }

    @Reference(policy= ReferencePolicy.DYNAMIC, cardinality= ReferenceCardinality.OPTIONAL)
    volatile Distributor distributor = null;

    @Reference
    JobManager jobManager;

    @Reference
    ResourceResolverFactory factory;

    @Reference
    ConfigurationMetadataProvider configMetadataProvider;

    Map<String, Class<? extends BasePipe>> registry;

    public static final String SLING_EVENT_TOPIC = "org/apache/sling/pipes/topic";

    private Configuration configuration;

    private Map<String, Object> serviceUser;

    private List<String> allowedUsers;

    private Map<String, PipeMonitor> monitoredPipes;

    public static final String PN_NBOUTPUTRESOURCES = "nbOutputResources";

    @Activate
    public void activate(Configuration configuration){
        this.configuration = configuration;
        serviceUser = configuration.serviceUser() != null ? Collections.singletonMap(SUBSERVICE, configuration.serviceUser()) : null;
        allowedUsers = Arrays.asList(configuration.authorizedUsers());
        registry = new HashMap<>();
        registerPipes();
        toggleJmxRegistration(this, PlumberMXBean.class.getName(), true);
        refreshMonitoredPipes();
    }

    /**
     * Register all pipes declared in pipe builder
     */
    void registerPipes(){
        registerPipe(ContainerPipe.RESOURCE_TYPE, ContainerPipe.class);
        registerPipe(ManifoldPipe.RESOURCE_TYPE, ManifoldPipe.class);
        for (Method method : PipeBuilderImpl.class.getDeclaredMethods()){
            PipeExecutor executor = method.getAnnotation(PipeExecutor.class);
            if (executor != null){
                registerPipe(executor.resourceType(), executor.pipeClass());
            }
        }
    }

    void checkPermissions(ResourceResolver context, String... permissions) {
        for (String permission : permissions) {
            if (context.getResource(permission) == null) {
                log.debug("error trying to check permission {}", permission);
                throw new AccessControlException("User has not the required permissions");
            }
        }
    }

    @Override
    public Map getServiceUser() {
        return serviceUser;
    }

    @Override
    public Map getContextAwareConfigurationMap(Resource currentResource) {
        return new ConfigurationMap(currentResource, configMetadataProvider);
    }

    @Override
    public @Nullable Resource getReferencedResource(Resource referrer, String reference) {
        ResourceResolver resolver = referrer.getResourceResolver();
        for (String path : configuration.referencesPaths()) {
            Resource target = resolver.getResource(path + SLASH + reference);
            if (target != null) {
                return target;
            }
        }
        return null;
    }

    @Deactivate
    public void deactivate(){
        toggleJmxRegistration(null, PlumberMXBean.class.getName(), false);
        if (monitoredPipes != null){
            for (String path : monitoredPipes.keySet()){
                toggleJmxRegistration(null, path, false);
            }
        }
    }

    /**
     * Toggle some mbean registration
     * @param name partial name that will be used for registration
     * @param register true to register, false to unregister
     */
    private void toggleJmxRegistration(Object instance, String name, boolean register){
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName oName = ObjectName.getInstance(String.format(MBEAN_NAME_FORMAT, name));
            if (register && !server.isRegistered(oName)) {
                server.registerMBean(instance, oName);
            }
            if (!register && server.isRegistered(oName)){
                server.unregisterMBean(oName);
            }
        } catch (Exception e) {
            log.error("unable to toggle mbean {} registration", name, e);
        }
    }

    @Override
    public Pipe getPipe(Resource resource) {
        return getPipe(resource, null);
    }

    @Override
    public Pipe getPipe(Resource resource, PipeBindings upperBindings) {
        if ((resource == null) || !registry.containsKey(resource.getResourceType())) {
            log.error("Pipe configuration resource is either null, or its type is not registered");
        } else {
            try {
                Class<? extends Pipe> pipeClass = registry.get(resource.getResourceType());
                return pipeClass.getDeclaredConstructor(Plumber.class, Resource.class, PipeBindings.class)
                        .newInstance(this, resource, upperBindings);
            } catch (Exception e) {
                log.error("Unable to properly instantiate the pipe configured in {}", resource.getPath(), e);
            }
        }
        return null;
    }

    @Override
    public void markWithJcrLastModified(@NotNull Pipe pipe, @NotNull Resource resource) {
        if (!pipe.isDryRun()) {
            Node node = resource.adaptTo(Node.class);
            if (node != null) {
                try {
                    node.setProperty(JCR_LASTMODIFIED, Calendar.getInstance());
                    node.setProperty(JCR_LAST_MODIFIED_BY, resource.getResourceResolver().getUserID());
                    if (configuration.mark_pipe_path()) {
                        node.setProperty(JCR_LAST_MODIFIED_BY_PIPE, pipe.getResource().getPath());
                    }
                } catch (RepositoryException e) {
                    log.debug("There is probably no way to mark that node, which we can't do much about", e);
                }
            }
        }
    }

    @Override
    public Map<String, Object> getBindingsFromRequest(SlingHttpServletRequest request, boolean writeAllowed) throws IOException {
        Map<String, Object> bindings = new HashMap<>();
        String dryRun = request.getParameter(BasePipe.DRYRUN_KEY);
        if (StringUtils.isNotBlank(dryRun) && !dryRun.equals(Boolean.FALSE.toString())) {
            bindings.put(BasePipe.DRYRUN_KEY, true);
        }
        String paramBindings = request.getParameter(PARAM_BINDINGS);
        if (StringUtils.isNotBlank(paramBindings)){
            try {
                bindings.putAll(JsonUtil.unbox(JsonUtil.parseObject(paramBindings)));
            } catch (Exception e){
                log.error("Unable to retrieve bindings information", e);
            }
        }
        RequestParameter fileParameter = request.getRequestParameter(PARAM_FILE);
        if (fileParameter != null){
            bindings.put(AbstractInputStreamPipe.BINDING_IS, fileParameter.getInputStream());
        }

        bindings.put(BasePipe.READ_ONLY, !writeAllowed);
        return bindings;
    }

    @Override
    public Job executeAsync(ResourceResolver resolver, String path, Map<String, Object> bindings) {
        if (allowedUsers.contains(resolver.getUserID())) {
            return executeAsync(path, bindings);
        }
        return null;
    }

    @Override
    public Job executeAsync(String path, Map<String, Object> bindings) {
        if (StringUtils.isBlank((String)serviceUser.get(SUBSERVICE))) {
            log.error("please configure plumber service user");
        }
        final Map<String, Object> props = new HashMap<>();
        props.put(SlingConstants.PROPERTY_PATH, path);
        props.put(PipeBindings.NN_ADDITIONALBINDINGS, bindings);
        return jobManager.addJob(SLING_EVENT_TOPIC, props);
    }

    @Override
    public ExecutionResult execute(ResourceResolver resolver, String path, Map additionalBindings, OutputWriter writer, boolean save) {
        Resource pipeResource = resolver.getResource(path);
        Pipe pipe = getPipe(pipeResource);
        if (pipe == null) {
            throw new IllegalArgumentException("unable to build pipe based on configuration at " + path);
        }
        return execute(resolver, pipe, additionalBindings, writer, save);
    }

    private ExecutionResult internalExecute(ResourceResolver resolver, OutputWriter writer, Pipe pipe) throws InterruptedException, PersistenceException {
        ExecutionResult result = new ExecutionResult(writer);
        for (Iterator<Resource> it = pipe.getOutput(); it.hasNext();){
            Resource resource = it.next();
            checkError(pipe, result);
            if (resource != null) {
                log.debug("[{}] retrieved {}", pipe.getName(), resource.getPath());
                result.addResultItem(resource);
                persist(resolver, pipe, result, resource);
            }
        }
        checkError(pipe, result);
        return result;
    }

    @Override
    public ExecutionResult execute(ResourceResolver resolver, Pipe pipe, Map additionalBindings, OutputWriter writer, boolean save) {
        checkPermissions(resolver, configuration.executionPermissionResource());
        boolean success = false;
        PipeMonitor monitor = null;
        ExecutionResult result = null;
        long start = System.currentTimeMillis();
        try {
            boolean readOnly = false;
            if (additionalBindings != null){
                pipe.getBindings().addBindings(additionalBindings);
                readOnly = (Boolean)additionalBindings.getOrDefault(BasePipe.READ_ONLY, false);
            }

            if (! pipe.isDryRun() && readOnly && pipe.modifiesContent()) {
                throw new IllegalArgumentException("This pipe modifies content, you should use a POST request");
            }
            log.debug("[{}] before execution hook is called", pipe);
            pipe.before();
            log.info("[{}] execution starts, save ({})", pipe, save);
            Resource confResource = pipe.getResource();
            writer.setPipe(pipe);
            if (isRunning(confResource)){
                throw new IllegalStateException("Pipe is already running");
            }
            monitor = monitoredPipes.get(confResource.getPath());
            writeStatus(pipe, STATUS_STARTED, null);
            resolver.commit();
            if (monitor != null){
                monitor.starts();
            }
            result = internalExecute(resolver, writer, pipe);
            if (save && pipe.modifiesContent()) {
                persist(resolver, pipe, result, null);
            }
            if (writer.autoClose()) {
                writer.ends();
            }
            if (monitor != null){
                monitor.ends();
                monitor.setLastResult(result);
            }
            success = true;
            return result;
        } catch (PersistenceException e) {
            throw new IllegalStateException("persistence error while executing pipe", e);
        } catch (InterruptedException ie) {
            log.error("execution interrupted", ie);
            Thread.currentThread().interrupt();
        } finally {
            try {
                writeStatus(pipe, STATUS_FINISHED, result);
                resolver.commit();
            } catch (PersistenceException e) {
                log.error("unable to make final save", e);
            }
            long length = System.currentTimeMillis() - start;
            String time = length < 1000 ? length + "ms" : (length / 1000) + "s";
            log.info("[{}] done executing in {}.", pipe.getName(), time);
            log.debug("[{}] after execution hook is called", pipe);
            pipe.after();
            if (!success && monitor != null){
                monitor.failed();
            }
        }
        //returning void result if we get there
        return new ExecutionResult(writer);
    }

    /**
     * check if current state contains error, and record it
     * @param pipe current pipe
     * @param result current result
     */
    void checkError(Pipe pipe, ExecutionResult result){
        String error = pipe.getBindings().popCurrentError();
        if (StringUtils.isNotBlank(error)){
            result.addError(error);
        }
    }

    private boolean shouldSave(ResourceResolver resolver, Pipe pipe, ExecutionResult result, Resource currentResource) {
        return  pipe.modifiesContent()
            && resolver.hasChanges()
            && !pipe.isDryRun()
            && (currentResource == null || result.size() % configuration.bufferSize() == 0);
    }

    /**
     * Persists pipe change if big enough, or ended, and eventually distribute changes
     * @param resolver resolver to use
     * @param pipe pipe at the origin of the changes,
     * @param result execution result object,
     * @param currentResource if running, null if ended
     * @throws PersistenceException in case save fails
     */
    void persist(ResourceResolver resolver, Pipe pipe, ExecutionResult result, Resource currentResource) throws PersistenceException, InterruptedException {
        if (shouldSave(resolver, pipe, result, currentResource)) {
            log.info("[{}] saving changes...", pipe.getName());
            writeStatus(pipe, currentResource == null ? STATUS_FINISHED : currentResource.getPath(), result);
            resolver.commit();
            if (currentResource == null && distributor != null && StringUtils.isNotBlank(pipe.getDistributionAgent())) {
                log.info("a distribution agent is configured, will try to distribute the changes");
                DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, true, result.getCurrentPathSet().toArray(new String[result.getCurrentPathSet().size()]));
                DistributionResponse response = distributor.distribute(pipe.getDistributionAgent(), resolver, request);
                log.info("distribution response : {}", response);
            }
            if (result.size() > configuration.bufferSize()) {
                //avoid too big foot print
                result.emptyCurrentSet();
            }
            if (configuration.sleep() > 0) {
                log.debug("sleeping for {}ms", configuration.sleep());
                Thread.sleep(configuration.sleep());
            }
        }
    }

    @Override
    public void registerPipe(String type, Class<? extends BasePipe> pipeClass) {
        registry.put(type, pipeClass);
    }

    @Override
    public boolean isTypeRegistered(String type) {
        return registry.containsKey(type);
    }

    /**
     * writes the status of the pipe, also update <code>PN_STATUS_MODIFIED</code> date
     * @param pipe target pipe
     * @param status status to write
     * @throws RepositoryException in case write goes wrong
     */
    void writeStatus(Pipe pipe, String status, ExecutionResult result){
        if (StringUtils.isNotBlank(status)){
            ModifiableValueMap vm = pipe.getResource().adaptTo(ModifiableValueMap.class);
            if( vm != null) {
                vm.put(PN_STATUS, status);
                vm.put(PN_NBOUTPUTRESOURCES, result != null ? result.size() : -1);
                Calendar cal = new GregorianCalendar();
                cal.setTime(new Date());
                vm.put(PN_STATUS_MODIFIED, cal);
            }
        }
    }

    @Override
    public String getStatus(Resource pipeResource) {
        Resource statusResource = pipeResource.getChild(PN_STATUS);
        if (statusResource != null){
            String status = statusResource.adaptTo(String.class);
            if (StringUtils.isNotBlank(status)){
                return status;
            }
        }
        return STATUS_FINISHED;
    }

    @Override
    public PipeBuilder newPipe(ResourceResolver resolver) {
        return new PipeBuilderImpl(resolver, this);
    }

    @Override
    public boolean isRunning(Resource pipeResource) {
        return !getStatus(pipeResource).equals(STATUS_FINISHED);
    }

    @Override
    public JobResult process(Job job) {
        try(ResourceResolver resolver = factory.getServiceResourceResolver(serviceUser)){
            String path = (String)job.getProperty(SlingConstants.PROPERTY_PATH);
            Map<String, Object> bindings = (Map)job.getProperty(PipeBindings.NN_ADDITIONALBINDINGS);
            OutputWriter writer = new JsonWriter();
            writer.starts();
            execute(resolver, path, bindings, writer, true);
            return JobResult.OK;
        } catch (LoginException e) {
            log.error("unable to retrieve resolver for executing scheduled pipe", e);
        } catch (Exception e) {
            log.error("failed to execute the pipe", e);
        }
        return JobResult.FAILED;
    }

    @Override
    public void refreshMonitoredPipes() {
        Map<String, PipeMonitor> map = new HashMap<>();
        getMonitoredPipes().stream().forEach(bean -> map.put(bean.getPath(), bean));
        if (monitoredPipes != null) {
            Collection<String> shouldBeRemoved = CollectionUtils.subtract(monitoredPipes.keySet(), map.keySet());
            for (String path : shouldBeRemoved){
                toggleJmxRegistration(null, path, false);
            }
        }
        monitoredPipes = map;
        for (Map.Entry<String, PipeMonitor> entry : monitoredPipes.entrySet()){
            toggleJmxRegistration(entry.getValue(), entry.getKey(), true);
        }
    }

    Collection<PipeMonitor> getMonitoredPipes() {
        Collection<PipeMonitor> beans = new ArrayList<>();
        if (serviceUser != null) {
            try (ResourceResolver resolver = factory.getServiceResourceResolver(serviceUser)) {
                for (Iterator<Resource> resourceIterator = resolver.findResources(MONITORED_PIPES_QUERY, Query.XPATH); resourceIterator.hasNext(); ) {
                    beans.add(new PipeMonitor(this, getPipe(resourceIterator.next())));
                }
            } catch (LoginException e) {
                log.error("unable to retrieve resolver for collecting exposed pipes", e);
            } catch (Exception e) {
                log.error("failed to execute the pipe", e);
            }
        } else {
            log.warn("no service user configured, pipes can't be monitored");
        }
        return beans;
    }

    /**
     * build a time + random based path under /var/pipes
     * @return full path of future Pipe
     */
    public String generateUniquePath() {
        final Calendar now = Calendar.getInstance();
        return PIPES_REPOSITORY_PATH + '/' + now.get(Calendar.YEAR) + '/' + now.get(Calendar.MONTH) + '/' + now.get(Calendar.DAY_OF_MONTH) + "/"
                + UUID.randomUUID().toString();
    }

    void cleanResourceAndEmptyParents(Resource resource) throws PersistenceException {
        log.debug("starting removal of {}", resource);
        Resource parent = resource.getParent();
        resource.getResourceResolver().delete(resource);
        if (parent != null && !parent.hasChildren() && !PIPES_REPOSITORY_PATH.equals(parent.getPath())) {
            cleanResourceAndEmptyParents(parent);
        }
    }

    void purge(ResourceResolver resolver, Instant now, int maxDays) throws PersistenceException {
        final Collection<String> pipesToRemove = new ArrayList<>();
        AbstractResourceVisitor visitor = new AbstractResourceVisitor() {
            @Override
            protected void visit(Resource res) {
                Calendar cal = res.getValueMap().get(PN_STATUS_MODIFIED, Calendar.class);
                if (cal != null && ChronoUnit.DAYS.between(cal.toInstant(), now) > maxDays) {
                    pipesToRemove.add(res.getPath());
                }
            }
        };
        visitor.accept(resolver.getResource(PIPES_REPOSITORY_PATH));
        if (!pipesToRemove.isEmpty()) {
            log.info("about to remove {} pipe instances", pipesToRemove.size());
            for (String path : pipesToRemove) {
                cleanResourceAndEmptyParents(resolver.getResource(path));
            }
            resolver.commit();
            log.info("purge done.");
        }
    }

    @Override
    public boolean allowAdditionalScripts() {
        return configuration.allow_additional_scripts();
    }

    @Override
    public void run() {
        if (serviceUser == null) {
            log.warn("no service user configured, will not be able to purge old pipe instances");
        } else {
            try (ResourceResolver resolver = factory.getServiceResourceResolver(serviceUser)) {
                log.info("Starting pipe purge based on a max age of {} days", configuration.maxAge());
                purge(resolver, Instant.now(), configuration.maxAge());
                resolver.commit();
            } catch (LoginException | PersistenceException e) {
                log.error("unable purge {}", PIPES_REPOSITORY_PATH, e);
            }
        }
    }

    @Override
    public Object evaluate(String expr, Bindings bindings) {
        JxltEngine internalEngine = new JxltEngine(bindings);
        return internalEngine.parse(expr);
    }
}