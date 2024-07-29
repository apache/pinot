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
package org.apache.pinot.core.data.function;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.function.InbuiltFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the JSON scalar transform functions
 */
public class JsonFunctionsTest {

  private void testFunction(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(functionExpression);
    Assert.assertEquals(evaluator.getArguments(), expectedArguments);
    Assert.assertEquals(evaluator.evaluate(row), expectedResult);
  }

  @Test(dataProvider = "jsonFunctionsDataProvider")
  public void testJsonFunctions(String functionExpression, List<String> expectedArguments, GenericRow row,
      Object expectedResult) {
    testFunction(functionExpression, expectedArguments, row, expectedResult);
  }

  @DataProvider(name = "jsonFunctionsDataProvider")
  public Object[][] jsonFunctionsDataProvider()
      throws IOException {
    List<Object[]> inputs = new ArrayList<>();

    // toJsonMapStr
    GenericRow row0 = new GenericRow();
    String jsonStr = "{\"k1\":\"foo\",\"k2\":\"bar\"}";
    row0.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"toJsonMapStr(jsonMap)", Lists.newArrayList("jsonMap"), row0, jsonStr});

    GenericRow row1 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row1.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"toJsonMapStr(jsonMap)", Lists.newArrayList("jsonMap"), row1, jsonStr});

    GenericRow row2 = new GenericRow();
    jsonStr = "{\"k1\":\"foo\",\"k2\":\"bar\"}";
    row2.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row2, jsonStr});

    GenericRow row3 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row3.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row3, jsonStr});

    GenericRow row4 = new GenericRow();
    jsonStr = "[{\"one\":1,\"two\":\"too\"},{\"one\":11,\"two\":\"roo\"}]";
    row4.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row4, jsonStr});

    GenericRow row5 = new GenericRow();
    jsonStr =
        "[{\"one\":1,\"two\":{\"sub1\":1.1,\"sub2\":1.2},\"three\":[\"a\",\"b\"]},{\"one\":11,\"two\":{\"sub1\":11.1,"
            + "\"sub2\":11.2},\"three\":[\"aa\",\"bb\"]}]";
    row5.putValue("jsonMap", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{"json_format(jsonMap)", Lists.newArrayList("jsonMap"), row5, jsonStr});

    GenericRow row6 = new GenericRow();
    jsonStr =
        "[{\"one\":1,\"two\":{\"sub1\":1.1,\"sub2\":1.2},\"three\":[\"a\",\"b\"]},{\"one\":11,\"two\":{\"sub1\":11.1,"
            + "\"sub2\":11.2},\"three\":[\"aa\",\"bb\"]}]";
    row6.putValue("jsonPathArray", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{
        "json_path_array(jsonPathArray, '$.[*].one')", Lists.newArrayList("jsonPathArray"), row6, new Object[]{1, 11}
    });

    GenericRow row7 = new GenericRow();
    jsonStr =
        "[{\"one\":1,\"two\":{\"sub1\":1.1,\"sub2\":1.2},\"three\":[\"a\",\"b\"]},{\"one\":11,\"two\":{\"sub1\":11.1,"
            + "\"sub2\":11.2},\"three\":[\"aa\",\"bb\"]}]";
    row7.putValue("jsonPathArray", JsonUtils.stringToObject(jsonStr, List.class));
    inputs.add(new Object[]{
        "json_path_array(jsonPathArray, '$.[*].three')", Lists.newArrayList("jsonPathArray"), row7,
        new Object[]{Arrays.asList("a", "b"), Arrays.asList("aa", "bb")}
    });

    GenericRow row8 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row8.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{
        "json_path_string(jsonPathString, '$.k3')", Lists.newArrayList("jsonPathString"), row8,
        "{\"sub1\":10,\"sub2\":1.0}"
    });

    GenericRow row9 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row9.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{
        "json_path_string(jsonPathString, '$.k4')", Lists.newArrayList("jsonPathString"), row9, "baz"
    });

    GenericRow row10 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row10.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{
        "json_path_long(jsonPathString, '$.k3.sub1')", Lists.newArrayList("jsonPathString"), row10, 10L
    });

    GenericRow row11 = new GenericRow();
    jsonStr = "{\"k3\":{\"sub1\":10,\"sub2\":1.0},\"k4\":\"baz\",\"k5\":[1,2,3]}";
    row11.putValue("jsonPathString", JsonUtils.stringToObject(jsonStr, Map.class));
    inputs.add(new Object[]{
        "json_path_double(jsonPathString, '$.k3.sub2')", Lists.newArrayList("jsonPathString"), row11, 1.0
    });
    return inputs.toArray(new Object[0][]);
  }

  @Test(description = "jsonFormat(Java null) should return Java null")
  public void jsonFormatWithJavaNullReturnsJavaNull() {
    GenericRow row = new GenericRow();
    row.putValue("jsonMap", null);
    testFunction("json_format(jsonMap)", Lists.newArrayList("jsonMap"), row, null);
  }

  @Test(description = "jsonFormat(JSON null) should return \"null\"")
  public void jsonFormatWithJsonNullReturnsStringNull()
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue("jsonMap", JsonUtils.stringToJsonNode("null"));
    testFunction("json_format(jsonMap)", Lists.newArrayList("jsonMap"), row, "null");
  }
}
