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
package org.apache.pinot.spi.data.function.evaluators;

import com.google.common.base.Splitter;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * An {@link ExpressionEvaluator} for evaluating schema transform expressions written in Groovy
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
public class GroovyExpressionEvaluator implements ExpressionEvaluator {

  private static final String GROOVY_EXPRESSION_PREFIX = "Groovy";
  private static final String GROOVY_FUNCTION_REGEX = "Groovy\\(\\{(?<script>.+)}(,(?<arguments>.+))?\\)";
  private static final Pattern GROOVY_FUNCTION_PATTERN = Pattern.compile(GROOVY_FUNCTION_REGEX, Pattern.CASE_INSENSITIVE);
  private static final String ARGUMENTS_GROUP_NAME = "arguments";
  private static final String SCRIPT_GROUP_NAME = "script";
  private static final String ARGUMENTS_SEPARATOR = ",";

  private List<String> _arguments;
  private String _script;

  public GroovyExpressionEvaluator(String transformExpression) {
    Matcher matcher = GROOVY_FUNCTION_PATTERN.matcher(transformExpression);
    if (matcher.matches()) {
      _script = matcher.group(SCRIPT_GROUP_NAME);

      String arguments = matcher.group(ARGUMENTS_GROUP_NAME);
      if (arguments != null) {
        _arguments = Splitter.on(ARGUMENTS_SEPARATOR).trimResults().splitToList(arguments);
      } else {
        _arguments = Collections.emptyList();
      }
    }
  }

  public static String getGroovyExpressionPrefix() {
    return GROOVY_EXPRESSION_PREFIX;
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    Map<String, Object> params = new HashMap<>();
    for (String argument : _arguments) {
      params.put(argument, genericRow.getValue(argument));
    }
    if (params.containsValue(null)) { // TODO: disallow evaluation of any of the params is null? Or give complete control to function?
      return null;
    } else {
      Binding binding = new Binding();
      for (String argument : _arguments) {
        binding.setVariable(argument, params.get(argument));
      }
      GroovyShell shell = new GroovyShell(binding);
      return shell.evaluate(_script);
    }
  }
}
