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
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.PipeExecutor;
import org.apache.sling.pipes.Plumber;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.sling.pipes.internal.CommandUtil.writeToMap;

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
    final Logger log = LoggerFactory.getLogger(GogoCommands.class);

    protected static final String SEPARATOR = "/";
    protected static final String PARAMS = "@";
    protected static final String INPUT = "-";
    protected static final String KEY_VALUE_SEP = "=";
    protected static final String KEY_NAME = "name";
    protected static final String KEY_PATH = "path";
    protected static final String KEY_EXPR = "expr";


    @Reference
    ResourceResolverFactory factory;

    @Reference
    Plumber plumber;

    Map<String, Method> methodMap;

    Map<String, PipeExecutor> executorMap;
    
    
    PrintStream print() {
        return System.out;
    }

    /**
     * run command handler
     * @param cmds string tokens coming with run command
     * @throws Exception in case anything went wrong
     */
    public void run(String... cmds) throws LoginException, InvocationTargetException, IllegalAccessException {
        try (ResourceResolver resolver = factory.getServiceResourceResolver(plumber.getServiceUser())) {
            PipeBuilder builder = parse(resolver, cmds);
            print().println(builder.run());
        }
    }

    /**
     * build command handler
     * @param cmds string tokens coming with build command
     * @throws Exception in case anything went wrong
     */
    public void build(String... cmds) throws LoginException, InvocationTargetException, IllegalAccessException, PersistenceException {
        try (ResourceResolver resolver = factory.getServiceResourceResolver(plumber.getServiceUser())) {
            PipeBuilder builder = parse(resolver, cmds);
            print().println(builder.build().getResource().getPath());
        }
    }

    /**
     * execute command handler
     * @param path pipe path
     * @param options string tokens coming with run command
     * @throws Exception in case anything went wrong
     */
    public void execute(String path, String... options) throws IOException, LoginException {
        String computedPath = INPUT.equals(path) ? IOUtils.toString(System.in).trim() : path;
        try (ResourceResolver resolver = factory.getServiceResourceResolver(plumber.getServiceUser())) {
            print().println(executeInternal(resolver, computedPath, options));
        }
    }

    /**
     * internal execution command handler
     * @param resolver resolver with which pipe will be executed
     * @param path pipe path to execute, {@code INPUT} for getting last token's output as path for things like build some / pipe | execute -
     * @param optionTokens different options tokens
     * @return Execution results
     * @throws Exception exception in case something goes wrong
     */
    protected ExecutionResult executeInternal(ResourceResolver resolver, String path, String... optionTokens) {
        Resource resource = resolver.getResource(path);
        if (resource == null){
            throw new IllegalArgumentException(String.format("%s resource does not exist", path));
        }
        Options options = getOptions(optionTokens);
        Map<String, Object> bMap = null;
        if (options.with != null) {
            bMap = new HashMap<>();
            writeToMap(bMap, options.with);
        }
        OutputWriter writer = new NopWriter();
        if (options.writer != null){
            writer = options.writer;
        }
        writer.starts();
        return plumber.execute(resolver, path, bMap, writer, true);
    }

    /**
     * help command handler
     */
    public void help(){
        print().format("\nSling Pipes Help\nAvailable commands are \n\n- execute <path> <options>(execute a pipe already built at a given path), if path is '-' then previous pipe token is used," +
                                "\n- build (build pipe as configured in arguments)" +
                                "\n- run (run pipe as configured in arguments)" +
                                "\n- help (print(). this help)" +
                                "\n\nfor pipe configured in argument, do 'pipe:<run|build|runAsync> <pipe token> (/ <pipe token> )*\n" +
                                "\n a <pipe token> is <pipe> <expr|conf>? (<options>)?" +
                                "\n <options> are (@ <option>)* form with <option> being either" +
                                "\n\t'name pipeName' (used in bindings), " +
                                "\n\t'expr pipeExpression' (when not directly as <args>)" +
                                "\n\t'path pipePath' (when not directly as <args>)" +
                                "\n\t'with key=value ...'" +
                                "\n\t'outputs key=value ...'" +
                                "\n and <pipe> is one of the following :\n");
        for (Map.Entry<String, PipeExecutor> entry : getExecutorMap().entrySet()){
            print().format("\t%s\t\t:\t%s%n", entry.getKey(), entry.getValue().description() );
        }
    }

    /**
     * @param resolver resource resolver with which pipe will build the pipe
     * @param cmds list of commands for building the pipe
     * @return PipeBuilder instance (that can be used to finalize the command)
     * @throws InvocationTargetException can happen in case the mapping with PB api went wrong
     * @throws IllegalAccessException can happen in case the mapping with PB api went wrong
     */
    protected PipeBuilder parse(ResourceResolver resolver, String...cmds) throws InvocationTargetException, IllegalAccessException {
        PipeBuilder builder = plumber.newPipe(resolver);
        for (Token token : parseTokens(cmds)){
            Method method = getMethodMap().get(token.pipeKey);
            if (method == null){
                throw new IllegalArgumentException(token.pipeKey + " is not a valid pipe");
            }
            if (isExpressionExpected(method)){
                method.invoke(builder, token.args.get(0));
            } else if (isConfExpected(method)){
                method.invoke(builder, (Object)keyValuesToArray(token.args));
            } else if (isWithoutExpectedParameter(method)){
                method.invoke(builder);
            }

            if (token.options != null){
                token.options.writeToBuilder(builder);
            }
        }
        return builder;

    }

    /**
     * builds utility maps
     */
    protected void computeMaps(){
        executorMap = new HashMap<>();
        methodMap = new HashMap<>();
        for (Method method : PipeBuilder.class.getDeclaredMethods()) {
            PipeExecutor executor = method.getAnnotation(PipeExecutor.class);
            if (executor != null) {
                methodMap.put(executor.command(), method);
                executorMap.put(executor.command(), executor);
            }
        }
    }

    /**
     * @return map of command to PB api method
     */
    protected Map<String, Method> getMethodMap() {
        if (methodMap == null) {
            computeMaps();
        }
        return methodMap;
    }

    /**
     * @return map of command to Annotation information around the PB api
     */
    protected Map<String, PipeExecutor> getExecutorMap() {
        if (executorMap == null) {
            computeMaps();
        }
        return executorMap;
    }

    /**
     * @param method corresponding PB api
     * @return true if the api does expect an expression (meaning a string)
     */
    protected boolean isExpressionExpected(Method method) {
        return method.getParameterCount() == 1 && method.getParameterTypes()[0].equals(String.class);
    }

    /**
     * @param method corresponding PB api
     * @return true if the api does expect a configuration (meaning a list of key value pairs)
     */
    protected boolean isConfExpected(Method method) {
        return method.getParameterCount() == 1 && method.getParameterTypes()[0].equals(Object[].class);
    }

    /**
     * @param method corresponding PB api
     * @return true if the api does not expect parameters
     */
    protected boolean isWithoutExpectedParameter(Method method){
        return method.getParameterCount() == 0;
    }

    /**
     * @param o list of key value strings key1:value1,key2:value2,...
     * @return String [] key1,value1,key2,value2,... corresponding to the pipe builder API
     */
    private String[] keyValuesToArray(List<String> o) {
        List<String> args = new ArrayList<>();
        for (String pair : o){
            args.addAll(Arrays.asList(pair.split(KEY_VALUE_SEP)));
        }
        return args.toArray(new String[args.size()]);
    }


    /**
     * @param commands full list of command tokens
     * @return Token list corresponding to the string ones
     */
    protected List<Token> parseTokens(String... commands) {
        List<Token> returnValue = new ArrayList<>();
        Token currentToken = new Token();
        returnValue.add(currentToken);
        List<String> currentList = new ArrayList<>();
        for (String token : commands){
            if (currentToken.pipeKey == null){
                currentToken.pipeKey = token;
            } else {
                switch (token){
                    case GogoCommands.SEPARATOR:
                        finishToken(currentToken, currentList);
                        currentList = new ArrayList<>();
                        currentToken = new Token();
                        returnValue.add(currentToken);
                        break;
                    case GogoCommands.PARAMS:
                        if (currentToken.args == null){
                            currentToken.args = currentList;
                            currentList = new ArrayList<>();
                        }
                        currentList.add(PARAMS);
                        break;
                    default:
                        currentList.add(token);
                }
            }
        }
        finishToken(currentToken, currentList);
        return returnValue;
    }

    /**
     * ends up processing of current token
     * @param currentToken token being processed
     * @param currentList list of argument that have been collected so far
     */
    protected void finishToken(Token currentToken, List<String> currentList){
        if (currentToken.args != null){
            //it means we have already parse args here, so we need to set current list as options
            currentToken.options = getOptions(currentList);
        } else {
            currentToken.args = currentList;
        }
        log.debug("current token : {}", currentToken);
    }

    /**
     * Pipe token, used to hold information of a "sub pipe" configuration
     */
    protected class Token {
        String pipeKey;
        List<String> args;
        Options options;

        @Override
        public String toString() {
            return "Token{" +
                    "pipeKey='" + pipeKey + '\'' +
                    ", args=" + args +
                    ", options=" + options +
                    '}';
        }
    }

    /**
     * @param tokens array of tokens
     * @return options from array
     */
    protected Options getOptions(String[] tokens){
        return getOptions(Arrays.asList(tokens));
    }

    /**
     * @param tokens list of tokens
     * @return options from token list
     */
    protected Options getOptions(List<String> tokens){
        return new Options(tokens);
    }

    /**
     * Options for a pipe execution
     */
    protected class Options {
        String name;
        String path;

        String expr;
        String[] with;
        OutputWriter writer;

        @Override
        public String toString() {
            return "Options{" +
                    "name='" + name + '\'' +
                    ", path='" + path + '\'' +
                    ", expr='" + expr + '\'' +
                    ", with=" + Arrays.toString(with) +
                    ", writer=" + writer +
                    '}';
        }



        void setOutputs(List<String> values) {
            this.writer = new JsonWriter();
            String[] list = keyValuesToArray(values);
            Map<String, Object> outputs = new HashMap<>();
            CommandUtil.writeToMap(outputs, list);
            this.writer.setCustomOutputs(outputs);
        }

        /**
         * Constructor
         * @param options string list from where options will be built
         */
        protected Options(List<String> options){
            Map<String, Object> optionMap = new HashMap<>();
            String currentKey = null;
            List<String> currentList = null;


            for (String optionToken : options) {
                if (PARAMS.equals(optionToken)){
                    finishOption(currentKey, currentList, optionMap);
                    currentList = new ArrayList<>();
                    currentKey = null;
                } else if (currentKey == null){
                    currentKey = optionToken;
                } else {
                    currentList.add(optionToken);
                }
            }
            finishOption(currentKey, currentList, optionMap);
            for (Map.Entry<String, Object> entry : optionMap.entrySet()){
                switch (entry.getKey()) {
                    case Pipe.PN_NAME :
                        this.name = (String)entry.getValue();
                        break;
                    case Pipe.PN_PATH :
                        this.path = (String)entry.getValue();
                        break;
                    case Pipe.PN_EXPR :
                        this.expr = (String)entry.getValue();
                        break;
                    case "with" :
                        this.with = keyValuesToArray((List<String>)entry.getValue());
                        break;
                    case "outputs" :
                        setOutputs((List<String>)entry.getValue());
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("%s is an unknown option", entry.getKey()));
                }
            }

        }

        /**
         * wrap up current option
         * @param currentKey option key
         * @param currentList list being processed
         * @param optionMap option map
         */
        protected void finishOption(String currentKey, List<String> currentList, Map<String, Object> optionMap){
            if (currentList != null){
                if (currentKey.equals(KEY_NAME) || currentKey.equals(KEY_EXPR) || currentKey.equals(KEY_PATH)) {
                    optionMap.put(currentKey, currentList.get(0));
                } else {
                    optionMap.put(currentKey, currentList);
                }
            }
        }

        /**
         * write options to current builder
         * @param builder current builder
         * @throws IllegalAccessException
         */
        void writeToBuilder(PipeBuilder builder) throws IllegalAccessException {
            if (StringUtils.isNotBlank(name)){
                builder.name(name);
            }
            if (StringUtils.isNotBlank(path)){
                builder.path(path);
            }
            if (StringUtils.isNotBlank(expr)){
                builder.expr(expr);
            }
            if (with != null){
                builder.with(with);
            }
        }
    }
}
