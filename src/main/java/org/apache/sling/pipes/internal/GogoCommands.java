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

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;

import org.apache.sling.pipes.CommandExecutor;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.Plumber;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;

@Component(immediate = true,
        service = GogoCommands.class,
        property = {
            "osgi.command.scope=pipe",
            "osgi.command.function=build",
            "osgi.command.function=run",
            "osgi.command.function=execute",
            "osgi.command.function=help"
        })
public class GogoCommands {

    static final String INPUT = "-";

    static final String COMMAND_HELP_START = "\nSling Pipes Help\nAvailable commands are \n\n- execute <path> <options>(execute a pipe already built at a given path), if path is '-' then previous pipe token is used," +
        "\n- build (build pipe as configured in arguments)" +
        "\n- run (run pipe as configured in arguments)" +
        "\n- help (print(). this help)" +
        "\n\nfor pipe configured in argument, do 'pipe:<run|build|runAsync> <pipe token> (/ <pipe token> )*\n";

    @Reference
    ResourceResolverFactory factory;

    @Reference
    CommandExecutor commandExecutor;

    @Reference
    Plumber plumber;

    @SuppressWarnings("squid:S106") // we want here output of the error
    PrintStream print() {
        return System.out;
    }

    /**
     * run command handler
     * @param cmds string tokens coming with run command
     * @throws LoginException in case configured service user is invalid,
     * @throws InvocationTargetException in case commands are misconfigured,
     * @throws IllegalAccessException in case this user can't access the pipe executions
     */
    public void run(String... cmds) throws LoginException, InvocationTargetException, IllegalAccessException {
        try (ResourceResolver resolver = factory.getServiceResourceResolver(plumber.getServiceUser())) {
            PipeBuilder builder = commandExecutor.parse(resolver, cmds);
            print().println(builder.run());
        }
    }

    /**
     * build command handler
     * @param cmds string tokens coming with build command
     * @throws LoginException in case configured service user is invalid,
     * @throws InvocationTargetException in case commands are misconfigured,
     * @throws IllegalAccessException in case this user can't access the pipe executions
     * @throws PersistenceException in case persistence of the pipe failed
     */
    public void build(String... cmds) throws LoginException, InvocationTargetException, IllegalAccessException, PersistenceException {
        try (ResourceResolver resolver = factory.getServiceResourceResolver(plumber.getServiceUser())) {
            PipeBuilder builder = commandExecutor.parse(resolver, cmds);
            print().println(builder.build().getResource().getPath());
        }
    }

    /**
     * execute command handler
     * @param path pipe path, {@code INPUT} for getting last token's output as path for things like build some / pipe | execute -
     * @param options string tokens coming with run command
     * @throws IOException if writing to the output fails,
     * @throws LoginException in case configured service user is invalid,
     */
    public void execute(String path, String... options) throws IOException, LoginException {
        String computedPath = INPUT.equals(path) ? IOUtils.toString(System.in).trim() : path;
        try (ResourceResolver resolver = factory.getServiceResourceResolver(plumber.getServiceUser())) {
            print().println(commandExecutor.execute(resolver, computedPath, options));
        }
    }

    /**
     * help command handler
     */
    public void help(){
        print().println(COMMAND_HELP_START + commandExecutor.help());
    }
}
