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

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/**
 * The <code>JsonExtractKeyTransformFunction</code> class implements the json path key transformation based on
 * <a href="https://goessner.net/articles/JsonPath/">Stefan Goessner JsonPath implementation.</a>.
 *
 * Please note, currently this method only works with String field. The values in this field should be Json String.
 *
 * Usage:
 * jsonExtractKey(jsonFieldName, 'jsonPath')
 * jsonExtractKey(jsonFieldName, 'jsonPath', 'optionalParameters')
 * <code>jsonFieldName</code> is the JSON String field/expression.
 * <code>jsonPath</code> is a JsonPath expression which used to read from JSON document
 * <code>optionalParameters</code> is extra optional parameters for this function, e.g. 'maxDepth=1;dotNotation=true'
 * <code>maxDepth</code> is an optional integer specifying the maximum depth to recurse
 * <code>dotNotation</code> is an optional boolean specifying output format (true=dot notation, false=JsonPath format)
 *
 */
public class JsonExtractKeyTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractKey";

  private TransformFunction _jsonFieldTransformFunction;
  private JsonPath _jsonPath;
  private boolean _isExtractAllKeys;
  private int _maxDepth = Integer.MAX_VALUE;
  private boolean _dotNotation = false;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are 2 or 3 arguments
    if (arguments.size() < 2 || arguments.size() > 3) {
      throw new IllegalArgumentException(
          "2 or 3 arguments are required for transform function: "
              + "jsonExtractKey(jsonFieldName, 'jsonPath', [optionalParameters])");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of jsonExtractKey transform function must be a single-valued column or a transform "
              + "function");
    }
    _jsonFieldTransformFunction = firstArgument;
    String jsonPathString = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral();
    _isExtractAllKeys = JsonFunctions.isExtractAllKeys(jsonPathString);
    _jsonPath = JsonPathCache.INSTANCE.getOrCompute(jsonPathString);

    // Handle the optional third argument (optionalParameters)
    if (arguments.size() == 3) {
      TransformFunction paramArgument = arguments.get(2);
      if (!(paramArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("The third argument (optionalParameters) must be a literal string");
      }
      JsonFunctions.JsonExtractFunctionParameters param = new JsonFunctions.JsonExtractFunctionParameters(
          ((LiteralTransformFunction) paramArgument).getStringLiteral());

      _maxDepth = param.getMaxDepth();
      _dotNotation = param.isDotNotation();
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_MV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      // Call the appropriate JsonFunctions method based on available parameters
      List values;
      try {
        if (_maxDepth != Integer.MAX_VALUE || _dotNotation) {
          if (_isExtractAllKeys) {
            values = JsonFunctions.jsonExtractAllKeysInternal(jsonStrings[i], _maxDepth, _dotNotation);
          } else {
            values = JsonFunctions.jsonExtractKeyInternal(jsonStrings[i], _jsonPath, _maxDepth, _dotNotation);
          }
        } else {
          if (_isExtractAllKeys) {
            values = JsonFunctions.jsonExtractAllKeysInternal(jsonStrings[i], Integer.MAX_VALUE, false);
          } else {
            values = JsonFunctions.jsonExtractKeyInternal(jsonStrings[i], _jsonPath);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // Convert list to String array
      _stringValuesMV[i] = new String[values.size()];
      for (int j = 0; j < values.size(); j++) {
        _stringValuesMV[i][j] = String.valueOf(values.get(j));
      }
    }
    return _stringValuesMV;
  }
}
