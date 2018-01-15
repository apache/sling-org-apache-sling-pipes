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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingConstants;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.Distributor;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.ContainerPipe;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.PlumberMXBean;
import org.apache.sling.pipes.ReferencePipe;
import org.apache.sling.pipes.internal.slingQuery.ChildrenPipe;
import org.apache.sling.pipes.internal.slingQuery.ClosestPipe;
import org.apache.sling.pipes.internal.slingQuery.FindPipe;
import org.apache.sling.pipes.internal.slingQuery.ParentPipe;
import org.apache.sling.pipes.internal.slingQuery.ParentsPipe;
import org.apache.sling.pipes.internal.slingQuery.SiblingsPipe;
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

import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
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

import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;
import static org.apache.sling.pipes.BasePipe.PN_STATUS;
import static org.apache.sling.pipes.BasePipe.PN_STATUS_MODIFIED;
import static org.apache.sling.pipes.BasePipe.STATUS_FINISHED;
import static org.apache.sling.pipes.BasePipe.STATUS_STARTED;

/**
 * implements plumber interface, registers default pipes, and provides execution facilities
 */
@Component(service = {Plumber.class, JobConsumer.class}, property = {
        JobConsumer.PROPERTY_TOPICS +"="+PlumberImpl.SLING_EVENT_TOPIC
})
@Designate(ocd = PlumberImpl.Configuration.class)
public class PlumberImpl implements Plumber, JobConsumer, PlumberMXBean {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    public static final int DEFAULT_BUFFER_SIZE = 1000;

    protected final static String PN_MONITORED = "monitored";
    protected final static String MONITORED_PIPES_QUERY = String.format("//element(*,nt:base)[@sling:resourceType='%s' and @%s]", ContainerPipe.RESOURCE_TYPE, PN_MONITORED);

    protected final static String MBEAN_NAME_FORMAT = "org.apache.sling.pipes:name=%s";

    @ObjectClassDefinition(name="Apache Sling Pipes : Plumber configuration")
    public @interface Configuration {
        @AttributeDefinition(description="Number of iterations after which plumber should saves a pipe execution")
        int bufferSize() default PlumberImpl.DEFAULT_BUFFER_SIZE;

        @AttributeDefinition(description="Number of milliseconds of sleep after each persistence")
        long sleep() default 0L;

        @AttributeDefinition(description="Name of service user, with appropriate rights, that will be used for async execution")
        String serviceUser();

        @AttributeDefinition(description="Users allowed to register async pipes")
        String[] authorizedUsers() default  {"admin"};
    }

    Map<String, Class<? extends BasePipe>> registry;

    public static final String SLING_EVENT_TOPIC = "org/apache/sling/pipes/topic";

    private Configuration configuration;

    private Map serviceUser;

    private List<String> allowedUsers;

    private Map<String, PipeMonitor> monitoredPipes;

    @Activate
    public void activate(Configuration configuration){
        this.configuration = configuration;
        serviceUser = Collections.singletonMap(SUBSERVICE, configuration.serviceUser());
        allowedUsers = Arrays.asList(configuration.authorizedUsers());
        registry = new HashMap<>();
        registerPipe(BasePipe.RESOURCE_TYPE, BasePipe.class);
        registerPipe(ContainerPipe.RESOURCE_TYPE, ContainerPipe.class);
        registerPipe(ChildrenPipe.RESOURCE_TYPE, ChildrenPipe.class);
        registerPipe(WritePipe.RESOURCE_TYPE, WritePipe.class);
        registerPipe(JsonPipe.RESOURCE_TYPE, JsonPipe.class);
        registerPipe(MultiPropertyPipe.RESOURCE_TYPE, MultiPropertyPipe.class);
        registerPipe(AuthorizablePipe.RESOURCE_TYPE, AuthorizablePipe.class);
        registerPipe(XPathPipe.RESOURCE_TYPE, XPathPipe.class);
        registerPipe(ReferencePipe.RESOURCE_TYPE, ReferencePipe.class);
        registerPipe(RemovePipe.RESOURCE_TYPE, RemovePipe.class);
        registerPipe(ParentsPipe.RESOURCE_TYPE, ParentsPipe.class);
        registerPipe(MovePipe.RESOURCE_TYPE, MovePipe.class);
        registerPipe(PathPipe.RESOURCE_TYPE, PathPipe.class);
        registerPipe(FilterPipe.RESOURCE_TYPE, FilterPipe.class);
        registerPipe(NotPipe.RESOURCE_TYPE, NotPipe.class);
        registerPipe(TraversePipe.RESOURCE_TYPE, TraversePipe.class);
        registerPipe(CsvPipe.RESOURCE_TYPE, CsvPipe.class);
        registerPipe(ParentPipe.RESOURCE_TYPE, ParentPipe.class);
        registerPipe(SiblingsPipe.RESOURCE_TYPE, SiblingsPipe.class);
        registerPipe(ClosestPipe.RESOURCE_TYPE, ClosestPipe.class);
        registerPipe(FindPipe.RESOURCE_TYPE, FindPipe.class);
        toggleJmxRegistration(this, PlumberMXBean.class.getName(), true);
        refreshMonitoredPipes();
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

    @Reference(policy= ReferencePolicy.DYNAMIC, cardinality= ReferenceCardinality.OPTIONAL)
    protected volatile Distributor distributor = null;

    @Reference
    JobManager jobManager;

    @Reference
    ResourceResolverFactory factory;

    @Override
    public Pipe getPipe(Resource resource) {
        if ((resource == null) || !registry.containsKey(resource.getResourceType())) {
            log.error("Pipe configuration resource is either null, or its type is not registered");
        } else {
            try {
                Class<? extends Pipe> pipeClass = registry.get(resource.getResourceType());
                return pipeClass.getDeclaredConstructor(Plumber.class, Resource.class).newInstance(this, resource);
            } catch (Exception e) {
                log.error("Unable to properly instantiate the pipe configured in {}", resource.getPath(), e);
            }
        }
        return null;
    }

    @Override
    public Job executeAsync(ResourceResolver resolver, String path, Map bindings) {
        if (allowedUsers.contains(resolver.getUserID())) {
            return executeAsync(path, bindings);
        }
        return null;
    }

    @Override
    public Job executeAsync(String path, Map bindings) {
        if (StringUtils.isBlank((String)serviceUser.get(SUBSERVICE))) {
            log.error("please configure plumber service user");
        }
        final Map props = new HashMap();
        props.put(SlingConstants.PROPERTY_PATH, path);
        props.put(PipeBindings.NN_ADDITIONALBINDINGS, bindings);
        return jobManager.addJob(SLING_EVENT_TOPIC, props);
    }

    @Override
    public ExecutionResult execute(ResourceResolver resolver, String path, Map additionalBindings, OutputWriter writer, boolean save) throws Exception {
        Resource pipeResource = resolver.getResource(path);
        Pipe pipe = getPipe(pipeResource);
        if (pipe == null) {
            throw new Exception("unable to build pipe based on configuration at " + path);
        }
        if (additionalBindings != null && (Boolean)additionalBindings.getOrDefault(BasePipe.READ_ONLY, true) && pipe.modifiesContent()) {
            throw new Exception("This pipe modifies content, you should use a POST request");
        }
        return execute(resolver, pipe, additionalBindings, writer, save);
    }

    @Override
    public ExecutionResult execute(ResourceResolver resolver, Pipe pipe, Map additionalBindings, OutputWriter writer, boolean save) throws Exception {
        boolean success = false;
        PipeMonitor monitor = null;
        try {
            log.info("[{}] execution starts, save ({})", pipe, save);
            if (additionalBindings != null && pipe instanceof ContainerPipe){
                pipe.getBindings().addBindings(additionalBindings);
            }
            Resource confResource = pipe.getResource();
            writer.setPipe(pipe);
            if (isRunning(confResource)){
                throw new RuntimeException("Pipe is already running");
            }
            monitor = monitoredPipes.get(confResource.getPath());
            writeStatus(pipe, STATUS_STARTED);
            resolver.commit();
            if (monitor != null){
                monitor.starts();
            }
            ExecutionResult result = new ExecutionResult(writer);
            for (Iterator<Resource> it = pipe.getOutput(); it.hasNext();){
                Resource resource = it.next();
                if (resource != null) {
                    log.debug("[{}] retrieved {}", pipe.getName(), resource.getPath());
                    result.addResultItem(resource);
                    persist(resolver, pipe, result, resource);
                }
            }
            if (save && pipe.modifiesContent()) {
                persist(resolver, pipe, result, null);
            }
            writer.ends();
            if (monitor != null){
                monitor.ends();
                monitor.setLastResult(result);
            }
            success = true;
            return result;
        } finally {
            writeStatus(pipe, STATUS_FINISHED);
            resolver.commit();
            log.info("[{}] done executing.", pipe.getName());
            if (!success && monitor != null){
                monitor.failed();
            }
        }
    }

    /**
     * Persists pipe change if big enough, or ended, and eventually distribute changes
     * @param resolver resolver to use
     * @param pipe pipe at the origin of the changes,
     * @param result execution result object,
     * @param currentResource if running, null if ended
     * @throws PersistenceException in case save fails
     */
    protected void persist(ResourceResolver resolver, Pipe pipe, ExecutionResult result, Resource currentResource) throws Exception {
        if  (pipe.modifiesContent() && resolver.hasChanges() && !pipe.isDryRun()){
            if (currentResource == null || result.size() % configuration.bufferSize() == 0){
                log.info("[{}] saving changes...", pipe.getName());
                writeStatus(pipe, currentResource == null ? STATUS_FINISHED : currentResource.getPath());
                resolver.commit();
                if (currentResource == null && distributor != null && StringUtils.isNotBlank(pipe.getDistributionAgent())) {
                    log.info("a distribution agent is configured, will try to distribute the changes");
                    DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, true, result.getCurrentPathSet().toArray(new String[result.getCurrentPathSet().size()]));
                    DistributionResponse response = distributor.distribute(pipe.getDistributionAgent(), resolver, request);
                    log.info("distribution response : {}", response);
                }
                if (result.size() > configuration.bufferSize()){
                    //avoid too big foot print
                    result.emptyCurrentSet();
                }
                if (configuration.sleep() > 0){
                    log.debug("sleeping for {}ms", configuration.sleep());
                    Thread.sleep(configuration.sleep());
                }
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
    protected void writeStatus(Pipe pipe, String status) throws RepositoryException {
        if (StringUtils.isNotBlank(status)){
            ModifiableValueMap vm = pipe.getResource().adaptTo(ModifiableValueMap.class);
            vm.put(PN_STATUS, status);
            Calendar cal = new GregorianCalendar();
            cal.setTime(new Date());
            vm.put(PN_STATUS_MODIFIED, cal);
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
        PipeBuilder builder = new PipeBuilderImpl(resolver, this);
        return builder;
    }

    @Override
    public boolean isRunning(Resource pipeResource) {
        return !getStatus(pipeResource).equals(STATUS_FINISHED);
    }

    @Override
    public JobResult process(Job job) {
        try(ResourceResolver resolver = factory.getServiceResourceResolver(serviceUser)){
            String path = (String)job.getProperty(SlingConstants.PROPERTY_PATH);
            Map bindings = (Map)job.getProperty(PipeBindings.NN_ADDITIONALBINDINGS);
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
        for (String path : monitoredPipes.keySet()){
            toggleJmxRegistration(monitoredPipes.get(path), path, true);
        }
    }

    protected Collection<PipeMonitor> getMonitoredPipes() {
        Collection<PipeMonitor> beans = new ArrayList<>();
        if (serviceUser != null) {
            try (ResourceResolver resolver = factory.getServiceResourceResolver(serviceUser)) {
                for (Iterator<Resource> resourceIterator = resolver.findResources(MONITORED_PIPES_QUERY, Query.XPATH); resourceIterator.hasNext(); ) {
                    beans.add(new org.apache.sling.pipes.internal.PipeMonitor(this, getPipe(resourceIterator.next())));
                }
            } catch (LoginException e) {
                log.error("unable to retrieve resolver for collecting exposed pipes", e);
            } catch (Exception e) {
                log.error("failed to execute the pipe", e);
            }
        }
        return beans;
    }
}