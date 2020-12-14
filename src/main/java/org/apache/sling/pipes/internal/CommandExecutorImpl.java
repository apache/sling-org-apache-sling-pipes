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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.request.RequestParameter;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.servlets.ServletResolverConstants;
import org.apache.sling.pipes.CommandExecutor;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.PipeExecutor;
import org.apache.sling.pipes.Plumber;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NOT_ACCEPTABLE;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.sling.pipes.PipeBindings.INJECTED_SCRIPT_REGEXP;
import static org.apache.sling.pipes.internal.CommandUtil.writeToMap;

import javax.json.JsonException;
import javax.servlet.Servlet;

@Component(service = {Servlet.class, CommandExecutor.class}, property= {
    ServletResolverConstants.SLING_SERVLET_RESOURCE_TYPES + "=" + CommandExecutorImpl.RESOURCE_TYPE,
    ServletResolverConstants.SLING_SERVLET_METHODS + "=POST",
    ServletResolverConstants.SLING_SERVLET_EXTENSIONS + "=json",
    ServletResolverConstants.SLING_SERVLET_EXTENSIONS + "=csv"
})
public class CommandExecutorImpl extends AbstractPlumberServlet implements CommandExecutor {
    final Logger log = LoggerFactory.getLogger(CommandExecutorImpl.class);
    public static final String RESOURCE_TYPE = "slingPipes/exec";
    static final String REQ_PARAM_FILE = "pipe_cmdfile";
    static final String REQ_PARAM_CMD = "pipe_cmd";
    static final String REQ_PARAM_HELP = "pipe_help";
    static final String CMD_LINE_PREFIX = "cmd_line_";
    static final String WHITE_SPACE_SEPARATOR = "\\s";
    static final String COMMENT_PREFIX = "#";
    static final String SEPARATOR = "|";
    static final String LINE_SEPARATOR = " ";
    static final String PARAMS = "@";
    static final String KEY_VALUE_SEP = "=";
    static final String FIRST_TOKEN = "first";
    static final String SECOND_TOKEN = "second";
    static final String CONFIGURATION_TOKEN = "(?<" + FIRST_TOKEN + ">[\\w/\\:]+)\\s*" + KEY_VALUE_SEP
        + "(?<" + SECOND_TOKEN + ">[(\\w*)|" + INJECTED_SCRIPT_REGEXP + "]+)";
    static final Pattern CONFIGURATION_PATTERN = Pattern.compile(CONFIGURATION_TOKEN);
    static final String KEY_NAME = "name";
    static final String KEY_PATH = "path";
    static final String KEY_EXPR = "expr";

    private static final String HELP_START =
        "\n a <pipe token> is <pipe> <expr|conf>? (<options>)?" +
        "\n <options> are (@ <option>)* form with <option> being either" +
        "\n\t'name pipeName' (used in bindings), " +
        "\n\t'expr pipeExpression' (when not directly as <args>)" +
        "\n\t'path pipePath' (when not directly as <args>)" +
        "\n\t'bindings key=value ...' (for setting pipe bindings) " +
        "\n\t'with key=value ...' (for setting pipe specific properties)" +
        "\n\t'outputs key=value ...' (for setting outputs)" +
        "\n and <pipe> is one of the following :\n";

    Map<String, Method> methodMap;
    Map<String, PipeExecutor> executorMap;

    String help;

    @Reference
    Plumber plumber;

    @Activate
    @Modified
    public void activate(){
        methodMap = null;
        executorMap = null;
        help = null;
    }

    boolean isCommandCandidate(String line) {
        return StringUtils.isNotBlank(line) && !line.startsWith(COMMENT_PREFIX);
    }

    List<String> getCommandList(SlingHttpServletRequest request) throws IOException {
        List<String> cmds = new ArrayList<>();
        if (request.getParameterMap().containsKey(REQ_PARAM_CMD)) {
            cmds.add(request.getParameter(REQ_PARAM_CMD));
        } else {
            RequestParameter paramFile = request.getRequestParameter(REQ_PARAM_FILE);
            if (paramFile != null) {
                InputStream is = paramFile.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String line;
                StringBuilder cmdBuilder = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    if (isCommandCandidate(line)) {
                        cmdBuilder.append(LINE_SEPARATOR + line.trim());
                    } else if (cmdBuilder.length() > 0){
                        cmds.add(cmdBuilder.toString().trim());
                        cmdBuilder = new StringBuilder();
                    }
                }
                if (cmdBuilder.length() > 0) {
                    cmds.add(cmdBuilder.toString().trim());
                }
            }
        }
        return cmds;
    }

    @Override
    protected void doPost(@NotNull SlingHttpServletRequest request, @NotNull SlingHttpServletResponse response) throws IOException {
        String currentCommand = null;
        PrintWriter writer = response.getWriter();
        try {
            if (request.getParameter(REQ_PARAM_HELP) != null) {
                writer.println(help());
            } else {
                ResourceResolver resolver = request.getResourceResolver();
                Map<String, Object> bindings = plumber.getBindingsFromRequest(request, true);
                List<String> cmds = getCommandList(request);
                if (cmds.isEmpty()) {
                    writer.println("No command to execute!");
                }
                short idxLine = 0;

                OutputWriter pipeWriter = getWriter(request, response);
                if (pipeWriter == null) {
                    pipeWriter = new NopWriter();
                }
                pipeWriter.disableAutoClose();
                pipeWriter.init(request, response);
                for (String command : cmds) {
                    if (StringUtils.isNotBlank(command)) {
                        currentCommand = command;
                        PipeBuilder pipeBuilder = parse(resolver, command.split(WHITE_SPACE_SEPARATOR));
                        Pipe pipe = pipeBuilder.build();
                        bindings.put(CMD_LINE_PREFIX + idxLine++, pipe.getResource().getPath());
                        plumber.execute(resolver, pipe, bindings, pipeWriter, true);
                    }
                }
                pipeWriter.ends();
            }
            writer.println("");
            response.setStatus(SC_OK);
        }
        catch (AccessControlException e) {
            response.setStatus(SC_FORBIDDEN);
            response.sendError(SC_FORBIDDEN);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            writer.println("Error executing " + currentCommand);
            e.printStackTrace(writer);
            response.setStatus(SC_INTERNAL_SERVER_ERROR);
            response.sendError(SC_INTERNAL_SERVER_ERROR);
            writer.println(help());
        }
        catch (IllegalArgumentException | JsonException e) {
            writer.println("Error executing " +  currentCommand);
            e.printStackTrace(writer);
            response.setStatus(SC_NOT_ACCEPTABLE);
            response.sendError(SC_NOT_ACCEPTABLE);
            writer.println(help());
        }
    }

    @Override
    public ExecutionResult execute(ResourceResolver resolver, String path, String... optionTokens) {
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

    @Override
    public PipeBuilder parse(ResourceResolver resolver, String...cmds) throws InvocationTargetException, IllegalAccessException {
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
        String[] bindings;
        OutputWriter writer;

        @Override
        public String toString() {
            return "Options{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", expr='" + expr + '\'' +
                ", with=" + Arrays.toString(with) +
                ", bindings=" + Arrays.toString(bindings) +
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
                    case "bindings" :
                        this.bindings = keyValuesToArray((List<String>) entry.getValue());
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
            if (bindings != null){
                builder.bindings(bindings);
            }
        }
    }

    /**
     * Pipe token, used to hold information of a "sub pipe" configuration
     */
    protected class Token {
        String pipeKey;
        List<String> args;
        CommandExecutorImpl.Options options;

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
     * @param commands full list of command tokens
     * @return Token list corresponding to the string ones
     */
    protected List<CommandExecutorImpl.Token> parseTokens(String... commands) {
        List<CommandExecutorImpl.Token> returnValue = new ArrayList<>();
        CommandExecutorImpl.Token currentToken = new CommandExecutorImpl.Token();
        returnValue.add(currentToken);
        List<String> currentList = new ArrayList<>();
        for (String token : commands){
            if (currentToken.pipeKey == null){
                currentToken.pipeKey = token;
            } else {
                switch (token){
                    case CommandExecutorImpl.SEPARATOR:
                        finishToken(currentToken, currentList);
                        currentList = new ArrayList<>();
                        currentToken = new CommandExecutorImpl.Token();
                        returnValue.add(currentToken);
                        break;
                    case CommandExecutorImpl.PARAMS:
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
     * @param o list of key value strings key1=value1,key2=value2,...
     * @return String [] key1,value1,key2,value2,... corresponding to the pipe builder API
     */
    String[] keyValuesToArray(List<String> o) {
        List<String> args = new ArrayList<>();
        for (String pair : o){
            Matcher matcher = CONFIGURATION_PATTERN.matcher(pair.trim());
            if (matcher.matches()) {
                args.add(matcher.group(FIRST_TOKEN));
                args.add(matcher.group(SECOND_TOKEN));
            }
        }
        return args.toArray(new String[args.size()]);
    }

    /**
     * help command handler
     */
    public String help(){
        if (StringUtils.isBlank(help)) {
            StringBuilder builder = new StringBuilder();
            builder.append(HELP_START);
            for (Map.Entry<String, PipeExecutor> entry : getExecutorMap().entrySet()) {
                builder.append(String.format("\t%s\t\t:\t%s%n", entry.getKey(), entry.getValue().description()));
            }
            help = builder.toString();
        }
        return help;
    }

}
