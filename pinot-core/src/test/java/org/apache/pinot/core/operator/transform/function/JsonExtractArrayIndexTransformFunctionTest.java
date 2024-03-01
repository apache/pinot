package org.apache.pinot.core.operator.transform.function;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


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
public class JsonExtractArrayIndexTransformFunctionTest extends BaseTransformFunctionTest {
  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());


  @Test
  public void testJsonExtractArrayIndexTransformFunction() {
//    String expressionStr, String jsonPathString, FieldSpec.DataType resultsDataType, boolean isSingleValue
    String expressionStr = "jsonExtractArrayIndex(jsonSV, '$.intArrayVal', 'INT_ARRAY', 0)";
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractIndexArrayTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractIndexArrayTransformFunction.FUNCTION_NAME);

    int[][] values = transformFunction.transformToIntValuesMV(_projectionBlock);
    JsonPath jsonPath = JsonPathCache.INSTANCE.getOrCompute("$.intArrayVal");
    for (int i = 0; i < values.length; i++) {
      List<Integer> data = JSON_PARSER_CONTEXT.parse(_jsonSVValues[i]).read(jsonPath, new TypeRef<List<Integer>>(){});
      for (int j = 0; j < values[i].length; j++) {
        Assert.assertEquals(values[i][j], data.get(j).intValue());
      }
    }
  }
}
