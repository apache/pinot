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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/**
 * The REGEXP_EXTRACT transform function takes maximum 4 arguments:
 * {@code REGEXP_EXTRACT(`value`, `regexp`[, `pos`, `group`])}
 * <ul>
 *   <li>value: a string used to match the regular expression.</li>
 *   <li>regex: the regular expression.</li>
 *   <li>group: the regular expression match group to extract.</li>
 *   <li>default_value: value when no match found, default to empty String.</li>
 * </ul>
 * Returns the first substring in `value` that matches the `regexp`.
 * <p>Returns empty String or the default_value if there is no match.
 * <p>If `group` is specified, the search returns a specific group of
 * the regexp in `value`, otherwise it returns the entire match.
 */
public class RegexpExtractTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "REGEXP_EXTRACT";

  private TransformFunction _valueFunction;
  private Pattern _regexp;
  private int _group;
  private String _defaultValue;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    Preconditions.checkArgument(arguments.size() >= 2 && arguments.size() <= 4,
        "REGEXP_EXTRACT takes between 2 to 4 arguments. See usage: "
            + "REGEXP_EXTRACT(`value`, `regexp`[, `group`[, `default_value`]]");
    _valueFunction = arguments.get(0);

    TransformFunction regexpFunction = arguments.get(1);
    Preconditions.checkState(regexpFunction instanceof LiteralTransformFunction,
        "`regexp` must be a literal regex expression.");
    _regexp = Pattern.compile(((LiteralTransformFunction) regexpFunction).getStringLiteral());

    if (arguments.size() >= 3) {
      TransformFunction groupFunction = arguments.get(2);
      Preconditions.checkState(groupFunction instanceof LiteralTransformFunction
              && ((LiteralTransformFunction) groupFunction).getIntLiteral() >= 0,
          "`group` must be a literal, non-negative integer.");
      _group = ((LiteralTransformFunction) groupFunction).getIntLiteral();
    } else {
      _group = 0;
    }

    if (arguments.size() == 4) {
      TransformFunction positionFunction = arguments.get(3);
      Preconditions.checkState(positionFunction instanceof LiteralTransformFunction,
          "`default_value` must be a literal expression.");
      _defaultValue = ((LiteralTransformFunction) regexpFunction).getStringLiteral();
    } else {
      _defaultValue = "";
    }
    _resultMetadata = STRING_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);
    String[] valuesSV = _valueFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      Matcher matcher = _regexp.matcher(valuesSV[i]);
      if (matcher.find() && matcher.groupCount() >= _group) {
        _stringValuesSV[i] = matcher.group(_group);
      } else {
        _stringValuesSV[i] = _defaultValue;
      }
    }
    return _stringValuesSV;
  }
}
