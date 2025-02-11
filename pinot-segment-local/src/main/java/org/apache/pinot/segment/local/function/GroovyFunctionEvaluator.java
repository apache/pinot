/**
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
package org.apache.pinot.segment.local.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link FunctionEvaluator} for evaluating transform function expressions of a Schema field spec written in Groovy.
 * GroovyShell is used to execute expressions.
 *
 * The transform expression must follow the convention Groovy({expression}, arguments1, argument2...)
 * For example:
 * "dimensionFieldSpecs": [
 *     {
 *       "name": "fullName",
 *       "dataType": "STRING",
 *       "transformFunction": "Groovy({firstName+' '+lastName}, firstName, lastName)"
 *     }
 *  ]
 */
public class GroovyFunctionEvaluator implements FunctionEvaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroovyFunctionEvaluator.class);

  private static final String GROOVY_EXPRESSION_PREFIX = "Groovy";
  private static final String GROOVY_FUNCTION_REGEX = "Groovy\\(\\{(?<script>.+)}(,(?<arguments>.+))?\\)";
  private static final Pattern GROOVY_FUNCTION_PATTERN =
      Pattern.compile(GROOVY_FUNCTION_REGEX, Pattern.CASE_INSENSITIVE);
  private static final String ARGUMENTS_GROUP_NAME = "arguments";
  private static final String SCRIPT_GROUP_NAME = "script";
  private static final String ARGUMENTS_SEPARATOR = ",";
  private static GroovyStaticAnalyzerConfig _config;
  private static AbstractMetrics _metrics;
  private static AbstractMetrics.Meter _securityViolationCounter;

  private final List<String> _arguments;
  private final int _numArguments;
  private final Binding _binding;
  private final Script _script;
  private final String _expression;

  public GroovyFunctionEvaluator(String closure) {
    _expression = closure;
    Matcher matcher = GROOVY_FUNCTION_PATTERN.matcher(closure);
    Preconditions.checkState(matcher.matches(), "Invalid transform expression: %s", closure);
    String arguments = matcher.group(ARGUMENTS_GROUP_NAME);
    if (arguments != null) {
      _arguments = Splitter.on(ARGUMENTS_SEPARATOR).trimResults().splitToList(arguments);
    } else {
      _arguments = Collections.emptyList();
    }
    _numArguments = _arguments.size();
    _binding = new Binding();
    final String scriptText = matcher.group(SCRIPT_GROUP_NAME);

    final GroovyStaticAnalyzerConfig groovyStaticAnalyzerConfig = getConfig();
    _script = createSafeShell(_binding, groovyStaticAnalyzerConfig).parse(scriptText);
  }

  public static String getGroovyExpressionPrefix() {
    return GROOVY_EXPRESSION_PREFIX;
  }

  /**
   * This will create a Groovy Shell that is configured with static syntax analysis. This static syntax analysis
   * will that any script which is run is restricted to a specific list of allowed operations, thus making it harder
   * to execute malicious code.
   *
   * @param binding Binding instance to be used by Groovy Shell.
   * @param groovyConfig GroovyStaticAnalyzerConfig instance to be used for static syntax analysis.
   * @return GroovyShell instance with static syntax analysis.
   */
  private GroovyShell createSafeShell(Binding binding, GroovyStaticAnalyzerConfig groovyConfig) {
    CompilerConfiguration config = new CompilerConfiguration();

    if (groovyConfig != null) {
      ImportCustomizer imports = new ImportCustomizer().addStaticStars("java.lang.Math");
      SecureASTCustomizer secure = new SecureASTCustomizer();

      secure.addExpressionCheckers(expression -> {
        if (expression instanceof MethodCallExpression) {
          MethodCallExpression method = (MethodCallExpression) expression;
          return !groovyConfig.getDisallowedMethodNames().contains(method.getMethodAsString());
        } else {
          return true;
        }
      });

      secure.setConstantTypesClassesWhiteList(GroovyStaticAnalyzerConfig.getDefaultAllowedTypes());
      secure.setImportsWhitelist(groovyConfig.getAllowedImports());
      secure.setStaticImportsWhitelist(groovyConfig.getAllowedImports());
      secure.setReceiversWhiteList(groovyConfig.getAllowedReceivers());

      // Block all * imports
      secure.setStaticStarImportsWhitelist(groovyConfig.getAllowedImports());
      secure.setStarImportsWhitelist(groovyConfig.getAllowedImports());

      // Allow all expression and token types
      secure.setExpressionsBlacklist(List.of());
      secure.setTokensBlacklist(List.of());

      secure.setMethodDefinitionAllowed(groovyConfig.isMethodDefinitionAllowed());
      secure.setIndirectImportCheckEnabled(true);
      secure.setClosuresAllowed(true);
      secure.setPackageAllowed(false);

      config.addCompilationCustomizers(imports, secure);
    }

    return new GroovyShell(binding, config);
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    boolean hasNullArgument = false;
    for (String argument : _arguments) {
      Object value = genericRow.getValue(argument);
      if (value == null) {
        hasNullArgument = true;
      }
      _binding.setVariable(argument, value);
    }
    try {
      return _script.run();
    } catch (SecurityException ex) {
      LOGGER.error("An insecure Groovy script execution caught: {}", _expression);
      incrementSecurityViolationCounter();
      throw ex;
    } catch (Exception e) {
      if (hasNullArgument) {
        return null;
      } else {
        throw e;
      }
    }
  }

  @Override
  public Object evaluate(Object[] values) {
    for (int i = 0; i < _numArguments; i++) {
      _binding.setVariable(_arguments.get(i), values[i]);
    }
    try {
      return _script.run();
    } catch (SecurityException ex) {
      LOGGER.error("An insecure Groovy script execution caught: {}", _expression);
      incrementSecurityViolationCounter();
      throw ex;
    }
  }

  @Override
  public String toString() {
    return _expression;
  }

  public static GroovyStaticAnalyzerConfig getConfig() {
    synchronized (GroovyFunctionEvaluator.class) {
      return _config;
    }
  }

  private static void incrementSecurityViolationCounter() {
    synchronized (GroovyFunctionEvaluator.class) {
      if (_metrics != null) {
        _metrics.addMeteredGlobalValue(_securityViolationCounter, 1);
      }
    }
  }

  public static void setMetrics(AbstractMetrics metrics, AbstractMetrics.Meter securityViolationCounter) {
    synchronized (GroovyFunctionEvaluator.class) {
      _metrics = metrics;
      _securityViolationCounter = securityViolationCounter;
    }
  }

  public static void configureGroovySecurity(String groovyASTConfig)
      throws Exception {
    try {
      if (groovyASTConfig != null) {
        setConfig(GroovyStaticAnalyzerConfig.fromJson(groovyASTConfig));
      } else {
        LOGGER.info("No Groovy Security Configuration found, Groovy static analysis is disabled");
      }
    } catch (Exception ex) {
      // If there was an issue reading the security configuration then send an exception to the startup routine.
      LOGGER.error("Failed to read config from ZK");
      throw ex;
    }
  }

  /**
   * Initialize or update the configuration for the Groovy Static Analyzer.
   * @param config GroovyStaticAnalyzerConfig instance to be used for static syntax analysis.
   */
  public static void setConfig(GroovyStaticAnalyzerConfig config)
      throws JsonProcessingException {
    synchronized (GroovyFunctionEvaluator.class) {
      _config = config;
    }
  }
}
