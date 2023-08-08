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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.JsonPathCache;
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
 * <code>jsonFieldName</code> is the Json String field/expression.
 * <code>jsonPath</code> is a JsonPath expression which used to read from JSON document
 *
 */
public class JsonExtractKeyTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractKey";

  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS)
          .build());

  private TransformFunction _jsonFieldTransformFunction;
  private JsonPath _jsonPath;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    // Check that there are exactly 2 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException(
          "Exactly 2 arguments are required for transform function: jsonExtractKey(jsonFieldName, 'jsonPath')");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of jsonExtractKey transform function must be a single-valued column or a transform "
              + "function");
    }
    _jsonFieldTransformFunction = firstArgument;
    _jsonPath = JsonPathCache.INSTANCE.getOrCompute(((LiteralTransformFunction) arguments.get(1)).getStringLiteral());
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
      List<String> values = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      _stringValuesMV[i] = values.toArray(new String[0]);
    }
    return _stringValuesMV;
  }
}
