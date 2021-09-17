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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;


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
  private String[] _stringOutputRegexMatches;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(
        arguments.size() >= 2 && arguments.size() <= 4,
        "REGEXP_EXTRACT takes between 2 to 4 arguments. See usage: "
            + "REGEXP_EXTRACT(`value`, `regexp`[, `group`, `default_value`]");
    _valueFunction = arguments.get(0);

    TransformFunction regexpFunction = arguments.get(1);
    Preconditions.checkState(
        regexpFunction instanceof LiteralTransformFunction,
        "`regexp` must be a literal regex expression.");
    _regexp = Pattern.compile(((LiteralTransformFunction) regexpFunction).getLiteral());

    if (arguments.size() >= 3) {
      TransformFunction groupFunction = arguments.get(2);
      Preconditions.checkState(groupFunction instanceof LiteralTransformFunction
              && Integer.parseInt(((LiteralTransformFunction) groupFunction).getLiteral()) >= 0,
          "`group` must be a literal, non-negative integer.");
      _group = Integer.parseInt(((LiteralTransformFunction) groupFunction).getLiteral());
    } else {
      _group = 0;
    }

    if (arguments.size() == 4) {
      TransformFunction positionFunction = arguments.get(3);
      Preconditions.checkState(positionFunction instanceof LiteralTransformFunction,
          "`default_value` must be a literal expression.");
      _defaultValue = ((LiteralTransformFunction) regexpFunction).getLiteral();
    } else {
      _defaultValue = "";
    }
    _resultMetadata = STRING_SV_NO_DICTIONARY_METADATA;
    _stringOutputRegexMatches = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    String[] valuesSV = _valueFunction.transformToStringValuesSV(projectionBlock);
    for (int i = 0; i < length; ++i) {
      Matcher matcher = _regexp.matcher(valuesSV[i]);
      if (matcher.find() && matcher.groupCount() >= _group) {
        _stringOutputRegexMatches[i] = matcher.group(_group);
      } else {
        _stringOutputRegexMatches[i] = _defaultValue;
      }
    }
    return _stringOutputRegexMatches;
  }
}
