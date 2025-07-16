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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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
 * jsonExtractKey(jsonFieldName, 'jsonPath', maxDepth)
 * jsonExtractKey(jsonFieldName, 'jsonPath', maxDepth, dotNotation)
 * <code>jsonFieldName</code> is the Json String field/expression.
 * <code>jsonPath</code> is a JsonPath expression which used to read from JSON document
 * <code>maxDepth</code> is an optional integer specifying the maximum depth to recurse
 * <code>dotNotation</code> is an optional boolean specifying output format (true=dot notation, false=JsonPath format)
 *
 */
public class JsonExtractKeyTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractKey";

  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS)
          .build());

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPath;
  private int _maxDepth = Integer.MAX_VALUE;
  private boolean _dotNotation = false;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are 2, 3, or 4 arguments
    if (arguments.size() < 2 || arguments.size() > 4) {
      throw new IllegalArgumentException(
          "2, 3, or 4 arguments are required for transform function: "
              + "jsonExtractKey(jsonFieldName, 'jsonPath', [maxDepth], [dotNotation])");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of jsonExtractKey transform function must be a single-valued column or a transform "
              + "function");
    }
    _jsonFieldTransformFunction = firstArgument;
    _jsonPath = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral();

    // Handle optional third argument (maxDepth)
    if (arguments.size() >= 3) {
      TransformFunction depthArgument = arguments.get(2);
      if (!(depthArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("The third argument (maxDepth) must be a literal integer");
      }
      try {
        _maxDepth = Integer.parseInt(((LiteralTransformFunction) depthArgument).getStringLiteral());
        if (_maxDepth <= 0) {
          throw new IllegalArgumentException("maxDepth must be a positive integer");
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("The third argument (maxDepth) must be a valid integer");
      }
    }

    // Handle optional fourth argument (dotNotation)
    if (arguments.size() == 4) {
      TransformFunction dotNotationArgument = arguments.get(3);
      if (!(dotNotationArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("The fourth argument (dotNotation) must be a literal boolean");
      }
      try {
        String dotNotationStr = ((LiteralTransformFunction) dotNotationArgument).getStringLiteral();
        if (!"true".equalsIgnoreCase(dotNotationStr) && !"false".equalsIgnoreCase(dotNotationStr)) {
          throw new IllegalArgumentException(
              "The fourth argument (dotNotation) must be a valid boolean ('true' or 'false')");
        }
        _dotNotation = Boolean.parseBoolean(dotNotationStr);
      } catch (Exception e) {
        throw new IllegalArgumentException("The fourth argument (dotNotation) must be a valid boolean");
      }
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
        if (_maxDepth != Integer.MAX_VALUE && _dotNotation) {
          // Call 4-parameter method
          values = JsonFunctions.jsonExtractKey((Object) jsonStrings[i], _jsonPath, _maxDepth, _dotNotation);
        } else if (_maxDepth != Integer.MAX_VALUE) {
          // Call 3-parameter method
          values = JsonFunctions.jsonExtractKey((Object) jsonStrings[i], _jsonPath, _maxDepth);
        } else {
          // Call 2-parameter method
          values = JsonFunctions.jsonExtractKey((Object) jsonStrings[i], _jsonPath);
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
