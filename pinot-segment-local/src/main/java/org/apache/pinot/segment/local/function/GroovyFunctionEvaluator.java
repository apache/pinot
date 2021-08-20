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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.data.readers.GenericRow;


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

  private static final String GROOVY_EXPRESSION_PREFIX = "Groovy";
  private static final String GROOVY_FUNCTION_REGEX = "Groovy\\(\\{(?<script>.+)}(,(?<arguments>.+))?\\)";
  private static final Pattern GROOVY_FUNCTION_PATTERN = Pattern.compile(GROOVY_FUNCTION_REGEX, Pattern.CASE_INSENSITIVE);
  private static final String ARGUMENTS_GROUP_NAME = "arguments";
  private static final String SCRIPT_GROUP_NAME = "script";
  private static final String ARGUMENTS_SEPARATOR = ",";

  private final List<String> _arguments;
  private final int _numArguments;
  private final Binding _binding;
  private final Script _script;

  public GroovyFunctionEvaluator(String closure) {
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
    _script = new GroovyShell(_binding).parse(matcher.group(SCRIPT_GROUP_NAME));
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
    for (String argument : _arguments) {
      Object value = genericRow.getValue(argument);
      if (value == null) {
        // FIXME: if any param is null a) exit OR b) assume function handles it ?
        return null;
      }
      _binding.setVariable(argument, value);
    }
    return _script.run();
  }

  @Override
  public Object evaluate(Object[] values) {
    for (int i = 0; i < _numArguments; i++) {
      _binding.setVariable(_arguments.get(i), values[i]);
    }
    return _script.run();
  }
}
