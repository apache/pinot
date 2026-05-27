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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class JsonExtractIndexTransformFunctionTest extends BaseTransformFunctionTest {
  private static final TypeRef<List<Integer>> INTEGER_LIST_TYPE = new TypeRef<List<Integer>>() {
  };
  private static final TypeRef<List<Long>> LONG_LIST_TYPE = new TypeRef<List<Long>>() {
  };
  private static final TypeRef<List<Float>> FLOAT_LIST_TYPE = new TypeRef<List<Float>>() {
  };
  private static final TypeRef<List<Double>> DOUBLE_LIST_TYPE = new TypeRef<List<Double>>() {
  };
  private static final TypeRef<List<String>> STRING_LIST_TYPE = new TypeRef<List<String>>() {
  };

  // Used to verify index value extraction
  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  @Test(dataProvider = "testJsonExtractIndexTransformFunction")
  public void testJsonExtractIndexTransformFunction(String expressionStr, String jsonPathString,
      DataType resultsDataType, boolean isSingleValue) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractIndexTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractIndexTransformFunction.FUNCTION_NAME);
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), resultsDataType);
    Assert.assertEquals(transformFunction.getResultMetadata().isSingleValue(), isSingleValue);
    JsonPath jsonPath = JsonPathCache.INSTANCE.getOrCompute(jsonPathString);
    if (isSingleValue) {
      switch (resultsDataType) {
        case INT:
          int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intValues[i], Integer.parseInt(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case LONG:
          long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longValues[i], Long.parseLong(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case FLOAT:
          float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatValues[i], Float.parseFloat(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case DOUBLE:
          double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleValues[i], Double.parseDouble(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(bigDecimalValues[i], new BigDecimal(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case STRING:
          String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringValues[i], getValueForKey(_jsonSVValues[i], jsonPath));
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    } else {
      switch (resultsDataType) {
        case INT:
          int[][] intValues = transformFunction.transformToIntValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Integer> values = getValueForKey(_jsonSVValues[i], jsonPath, INTEGER_LIST_TYPE);
            Assert.assertEquals(intValues[i].length, values.size());
            for (int j = 0; j < intValues[i].length; j++) {
              Assert.assertEquals(intValues[i][j], values.get(j));
            }
          }
          break;
        case LONG:
          long[][] longValues = transformFunction.transformToLongValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Long> values = getValueForKey(_jsonSVValues[i], jsonPath, LONG_LIST_TYPE);
            Assert.assertEquals(longValues[i].length, values.size());
            for (int j = 0; j < longValues[i].length; j++) {
              Assert.assertEquals(longValues[i][j], values.get(j));
            }
          }
          break;
        case FLOAT:
          float[][] floatValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Float> values = getValueForKey(_jsonSVValues[i], jsonPath, FLOAT_LIST_TYPE);
            Assert.assertEquals(floatValues[i].length, values.size());
            for (int j = 0; j < floatValues[i].length; j++) {
              Assert.assertEquals(floatValues[i][j], values.get(j));
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Double> values = getValueForKey(_jsonSVValues[i], jsonPath, DOUBLE_LIST_TYPE);
            Assert.assertEquals(doubleValues[i].length, values.size());
            for (int j = 0; j < doubleValues[i].length; j++) {
              Assert.assertEquals(doubleValues[i][j], values.get(j));
            }
          }
          break;
        case STRING:
          String[][] stringValues = transformFunction.transformToStringValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<String> values = getValueForKey(_jsonSVValues[i], jsonPath, STRING_LIST_TYPE);
            Assert.assertEquals(stringValues[i].length, values.size());
            for (int j = 0; j < stringValues[i].length; j++) {
              Assert.assertEquals(stringValues[i][j], values.get(j));
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    }
  }

  @DataProvider(name = "testJsonExtractIndexTransformFunction")
  public Object[][] testJsonExtractIndexTransformFunctionDataProvider() {
    List<Object[]> testArguments = new ArrayList<>();
    // Without default value
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT')", JSON_STRING_SV_COLUMN,
            "$.intVal"), "$.intVal", DataType.INT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG')", JSON_STRING_SV_COLUMN,
            "$.longVal"), "$.longVal", DataType.LONG, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','FLOAT')", JSON_STRING_SV_COLUMN,
            "$.floatVal"), "$.floatVal", DataType.FLOAT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE')", JSON_STRING_SV_COLUMN,
            "$.doubleVal"), "$.doubleVal", DataType.DOUBLE, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','BIG_DECIMAL')", JSON_STRING_SV_COLUMN,
            "$.bigDecimalVal"), "$.bigDecimalVal", DataType.BIG_DECIMAL, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING')", JSON_STRING_SV_COLUMN,
            "$.stringVal"), "$.stringVal", DataType.STRING, true
    });

    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT')", JSON_STRING_SV_COLUMN,
            "$.intVals[0]"), "$.intVals[0]", DataType.INT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG')", JSON_STRING_SV_COLUMN,
            "$.longVals[1]"), "$.longVals[1]", DataType.LONG, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','FLOAT')", JSON_STRING_SV_COLUMN,
            "$.floatVals[0]"), "$.floatVals[0]", DataType.FLOAT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE')", JSON_STRING_SV_COLUMN,
            "$.doubleVals[1]"), "$.doubleVals[1]", DataType.DOUBLE, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','BIG_DECIMAL')", JSON_STRING_SV_COLUMN,
            "$.bigDecimalVals[0]"), "$.bigDecimalVals[0]", DataType.BIG_DECIMAL, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING')", JSON_STRING_SV_COLUMN,
            "$.stringVals[1]"), "$.stringVals[1]", DataType.STRING, true
    });

    addMvTests(testArguments);
    return testArguments.toArray(new Object[0][]);
  }

  private void addMvTests(List<Object[]> testArguments) {
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.intVals[*]"), "$.intVals[*]", DataType.INT, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.longVals[*]"), "$.longVals[*]", DataType.LONG, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.doubleVals[*]"), "$.doubleVals[*]", DataType.DOUBLE, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.stringVals[*]"), "$.stringVals[*]", DataType.STRING, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.arrayField[*].arrIntField"), "$.arrayField[*].arrIntField", DataType.INT, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.arrayField[*].arrStringField"), "$.arrayField[*].arrStringField", DataType.STRING, false
    });

    // MV with filters
    testArguments.add(new Object[]{
        String.format(
            "jsonExtractIndex(%s,'%s','INT_ARRAY', '[]', 'REGEXP_LIKE(\"$.arrayField[*].arrStringField\", ''.*y.*'')')",
            JSON_STRING_SV_COLUMN,
            "$.arrayField[*].arrIntField"), "$.arrayField[?(@.arrStringField =~ /.*y.*/)].arrIntField", DataType.INT,
        false
    });

    testArguments.add(new Object[]{
        String.format(
            "jsonExtractIndex(%s,'%s','STRING_ARRAY', '[]', '\"$.arrayField[*].arrIntField\" > 2')",
            JSON_STRING_SV_COLUMN,
            "$.arrayField[*].arrStringField"), "$.arrayField[?(@.arrIntField > 2)].arrStringField", DataType.STRING,
        false
    });
  }

  @Test(dataProvider = "testJsonExtractIndexDefaultValue")
  public void testJsonExtractIndexDefaultValue(String expressionStr, String jsonPathString, DataType resultsDataType,
      boolean isSingleValue, Object expectedDefaultValue) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractIndexTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractIndexTransformFunction.FUNCTION_NAME);
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), resultsDataType);
    Assert.assertEquals(transformFunction.getResultMetadata().isSingleValue(), isSingleValue);
    JsonPath jsonPath = JsonPathCache.INSTANCE.getOrCompute(jsonPathString);
    if (isSingleValue) {
      switch (resultsDataType) {
        case INT:
          int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intValues[i], expectedDefaultValue);
          }
          break;
        case LONG:
          long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longValues[i], expectedDefaultValue);
          }
          break;
        case FLOAT:
          float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatValues[i], expectedDefaultValue);
          }
          break;
        case DOUBLE:
          double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleValues[i], expectedDefaultValue);
          }
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(bigDecimalValues[i], expectedDefaultValue);
          }
          break;
        case STRING:
          String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringValues[i], expectedDefaultValue);
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    } else {
      switch (resultsDataType) {
        case INT:
          int[][] intValues = transformFunction.transformToIntValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intValues[i], expectedDefaultValue);
          }
          break;
        case LONG:
          long[][] longValues = transformFunction.transformToLongValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longValues[i], expectedDefaultValue);
          }
          break;
        case FLOAT:
          float[][] floatValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatValues[i], expectedDefaultValue);
          }
          break;
        case DOUBLE:
          double[][] doubleValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleValues[i], expectedDefaultValue);
          }
          break;
        case STRING:
          String[][] stringValues = transformFunction.transformToStringValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringValues[i], expectedDefaultValue);
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    }
  }

  @DataProvider(name = "testJsonExtractIndexDefaultValue")
  public Object[][] testJsonExtractIndexDefaultValueDataProvider() {
    List<Object[]> testArguments = new ArrayList<>();
    // With default value
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.INT, true, 0
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.LONG, true, 0L
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','FLOAT',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.FLOAT, true, (float) 0
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.DOUBLE, true, (double) 0
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','BIG_DECIMAL',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.BIG_DECIMAL, true, new BigDecimal(0)
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING','null')", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.STRING, true, "null"
    });
    addMvDefaultValueTests(testArguments);
    return testArguments.toArray(new Object[0][]);
  }

  private void addMvDefaultValueTests(List<Object[]> testArguments) {
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT_ARRAY', '%s')", JSON_STRING_SV_COLUMN, "$.noField",
            "[1, 2, 3]"), "$.noField", DataType.INT, false, new Integer[]{1, 2, 3}
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG_ARRAY', '%s')", JSON_STRING_SV_COLUMN, "$.noField",
            "[1, 5, 6]"), "$.noField", DataType.LONG, false, new Long[]{1L, 5L, 6L}
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','FLOAT_ARRAY', '%s')", JSON_STRING_SV_COLUMN, "$.noField",
            "[1.2, 3.1, 1.6]"), "$.noField", DataType.FLOAT, false, new Float[]{1.2f, 3.1f, 1.6f}
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE_ARRAY', '%s')", JSON_STRING_SV_COLUMN, "$.noField",
            "[1.5, 3.4, 1.6]"), "$.noField", DataType.DOUBLE, false, new Double[]{1.5d, 3.4d, 1.6d}
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING_ARRAY', '%s')", JSON_STRING_SV_COLUMN, "$.noField",
            "[\"randomString1\", \"randomString2\"]"), "$.noField", DataType.STRING, false,
        new String[]{"randomString1", "randomString2"}
    });
  }

  // get value for key, excluding nested
  private String getValueForKey(String blob, JsonPath path) {
    Object out = JSON_PARSER_CONTEXT.parse(blob).read(path);
    if (out == null || out instanceof HashMap || out instanceof Object[]) {
      return null;
    }
    return out.toString();
  }

  private <T> T getValueForKey(String blob, JsonPath path, TypeRef<T> typeRef) {
    return JSON_PARSER_CONTEXT.parse(blob).read(path, typeRef);
  }

  // ============================================================================================
  // Country/click null-handling tests for jsonExtractIndex (issue #18568).
  //
  // The SV null-handling fix: with `enableNullHandling = true` and no default literal, an
  // unresolved JSON path must surface as SQL NULL instead of throwing.
  //
  // All tests below share a single `clicks` table fixture with two JSON-indexed columns and four
  // rows. The fixture mirrors the one in JsonExtractScalarTransformFunctionTest so that, modulo
  // the function name, expected results are identical — `jsonExtractIndex` and
  // `jsonExtractScalar` must agree on null semantics after the fix.
  //
  //   `flatJson`   — 1-level scalar shapes. Each row exercises one case: resolved value, explicit
  //                  JSON null, missing key, empty document.
  //   `nestedJson` — multi-level shapes (nested object, array, array of objects) with mixed
  //                  resolved / null / missing data at depth.
  //
  // Tests are grouped by query shape in this order: projection → DISTINCT → GROUP BY. Each section
  // covers three sub-cases, in order:
  //
  //   .1  NH on, 3-arg                          — unresolved rows surface as SQL NULL (E4 fix)
  //   .2  NH on, 4-arg with a non-null default   — default wins over NH placeholder
  //   .3  NH off                                  — legacy throw preserved
  //
  // Unlike the scalar function, the index function has no `_defaultIsNull` path: a `NULL` literal
  // as the 4-arg default fails at init (numeric) or silently becomes empty string (STRING) — both
  // pre-existing quirks outside the scope of this PR. Result rows in every assertion are ordered
  // by the projected expression ASC NULLS LAST so the comparison is deterministic across the
  // framework's segment-duplication behavior (one segment becomes two, so per-row counts ×2).
  // ============================================================================================

  protected File _baseDir;

  @BeforeClass
  void createBaseDir() {
    try {
      _baseDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @AfterClass
  void destroyBaseDir()
      throws IOException {
    if (_baseDir != null) {
      FileUtils.deleteDirectory(_baseDir);
    }
  }

  // ---------------- Fixture ----------------
  //
  // Pretty-printed contents of the 4-row × 2-column fixture:
  //
  //   row 0 — fully populated:
  //     flatJson:   { "country": "US", "clicks": 5 }
  //     nestedJson: { "location": {"city": "SF", "country": "US"},
  //                   "tags":     ["red", "blue", "green"],
  //                   "events":   [{"country": "US"}, {"country": "CA"}] }
  //
  //   row 1 — partial / explicit-null at depth:
  //     flatJson:   { "country": "CA", "clicks": 3 }
  //     nestedJson: { "location": {"city": "Tor"},          // <-- no country key
  //                   "tags":     ["green"],                 // <-- only one element
  //                   "events":   [{"country": null}] }     // <-- inner explicit null
  //
  //   row 2 — every field explicit JSON null:
  //     flatJson:   { "country": null, "clicks": null }
  //     nestedJson: { "location": null, "tags": null, "events": null }
  //
  //   row 3 — empty documents:
  //     flatJson:   {}
  //     nestedJson: {}

  private static final Object[][] COUNTRY_CLICK_FIXTURE = {
      // {flatJson, nestedJson}
      {
          "{\"country\":\"US\",\"clicks\":5}",
          "{\"location\":{\"city\":\"SF\",\"country\":\"US\"},"
              + "\"tags\":[\"red\",\"blue\",\"green\"],"
              + "\"events\":[{\"country\":\"US\"},{\"country\":\"CA\"}]}"
      },
      {
          "{\"country\":\"CA\",\"clicks\":3}",
          "{\"location\":{\"city\":\"Tor\"},"
              + "\"tags\":[\"green\"],"
              + "\"events\":[{\"country\":null}]}"
      },
      {
          "{\"country\":null,\"clicks\":null}",
          "{\"location\":null,\"tags\":null,\"events\":null}"
      },
      {"{}", "{}"}
  };

  // ---------------- Helper ----------------
  //
  // `jsonExtractIndex` requires the column to have a JSON index. The helper wires a JSON index
  // onto both columns via FieldConfig — same shape as the JSON index in
  // BaseTransformFunctionTest.getTableConfig().

  private FluentQueryTest.OnFirstInstance givenCountryClickTable(boolean nullHandling) {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("clicks")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("flatJson", DataType.JSON)
        .addDimensionField("nestedJson", DataType.JSON)
        .build();
    ObjectNode jsonIndexNode = JsonNodeFactory.instance.objectNode();
    jsonIndexNode.set("json", JsonNodeFactory.instance.objectNode());
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig("flatJson", FieldConfig.EncodingType.DICTIONARY, null, null, null, null, jsonIndexNode, null,
            null),
        new FieldConfig("nestedJson", FieldConfig.EncodingType.DICTIONARY, null, null, null, null, jsonIndexNode, null,
            null));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("clicks")
        .setFieldConfigList(fieldConfigs)
        .build();
    return FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(nullHandling)
        .givenTable(schema, tableConfig)
        .onFirstInstance(COUNTRY_CLICK_FIXTURE);
  }

  // ============================================================================================
  // 1. Projection
  // ============================================================================================

  /**
   * 1.1 — NH on, 3-arg. The SV transform must surface SQL NULL for unresolved rows and pass
   * resolved values through.
   * <pre>
   *   Example — `flatJson.country` (STRING):
   *     per-row: row 0 -> "US", row 1 -> "CA", row 2 -> NULL (explicit), row 3 -> NULL (missing)
   *     Query:  SET enableNullHandling = true;
   *             SELECT jsonExtractIndex(flatJson, '$.country', 'STRING') AS c
   *             FROM clicks ORDER BY c ASC NULLS LAST
   *     Result (×2 dup): "CA", "CA", "US", "US", NULL, NULL, NULL, NULL
   * </pre>
   */
  @Test(dataProvider = "projectionCases")
  public void testProjectionNullHandlingOn(String column, String jsonPath, String resultsType,
      Object[][] expectedRows, String label) {
    String expr = String.format("jsonExtractIndex(%s, '%s', '%s')", column, jsonPath, resultsType);
    givenCountryClickTable(true)
        .whenQuery(String.format("SELECT %s FROM clicks ORDER BY %s ASC NULLS LAST", expr, expr))
        .thenResultIs(expectedRows);
  }

  /**
   * 1.2 — NH on, 4-arg with non-null default. The SV transform's priority is: real default >
   * null-handling placeholder > throw. The user-supplied default surfaces for unresolved rows;
   * no null bit is set in the bitmap.
   */
  @Test(dataProvider = "defaultPrecedenceProjectionCases")
  public void testProjectionDefaultBeatsNullHandlingPlaceholder(String column, String jsonPath,
      String defaultLiteral, Object[][] expectedRows, String label) {
    String expr = String.format(
        "jsonExtractIndex(%s, '%s', 'STRING', %s)", column, jsonPath, defaultLiteral);
    givenCountryClickTable(true)
        .whenQuery(String.format("SELECT %s AS c FROM clicks ORDER BY c ASC", expr))
        .thenResultIs(expectedRows);
  }

  /**
   * 1.3 — NH off: any unresolved row throws. One representative path; throw is path-independent.
   * The index function's error message differs from the scalar's ("Illegal Json Path" vs "Cannot
   * resolve JSON path").
   */
  @Test
  public void testProjectionNullHandlingOffThrows() {
    try {
      givenCountryClickTable(false)
          .whenQuery("SELECT jsonExtractIndex(flatJson, '$.country', 'STRING') FROM clicks")
          .thenResultIs(new Object[]{"unused"});
      Assert.fail("Expected projection to fail when null handling is off and rows are unresolved");
    } catch (AssertionError e) {
      Assertions.assertThat(e.getMessage()).contains("Illegal Json Path");
    }
  }

  /** 1.1 cases — `(column, jsonPath, resultsType, expected 8-row result, label)`, ordered ASC NULLS LAST. */
  @DataProvider(name = "projectionCases")
  public static Object[][] projectionCases() {
    return new Object[][]{
        // ----- flatJson: 1-level scalar shape -----
        // $.country: row0="US", row1="CA", row2=null (explicit), row3=null (missing).
        {"flatJson", "$.country", "STRING", new Object[][]{
            {"CA"}, {"CA"}, {"US"}, {"US"}, {null}, {null}, {null}, {null}
        }, "flatJson.country STRING"},
        // $.clicks across all 6 numeric SV types: row0=5, row1=3, row2=null, row3=null.
        {"flatJson", "$.clicks", "INT", new Object[][]{
            {3}, {3}, {5}, {5}, {null}, {null}, {null}, {null}
        }, "flatJson.clicks INT"},
        {"flatJson", "$.clicks", "LONG", new Object[][]{
            {3L}, {3L}, {5L}, {5L}, {null}, {null}, {null}, {null}
        }, "flatJson.clicks LONG"},
        {"flatJson", "$.clicks", "FLOAT", new Object[][]{
            {3f}, {3f}, {5f}, {5f}, {null}, {null}, {null}, {null}
        }, "flatJson.clicks FLOAT"},
        {"flatJson", "$.clicks", "DOUBLE", new Object[][]{
            {3d}, {3d}, {5d}, {5d}, {null}, {null}, {null}, {null}
        }, "flatJson.clicks DOUBLE"},
        // BIG_DECIMAL is formatted as String by the broker.
        {"flatJson", "$.clicks", "BIG_DECIMAL", new Object[][]{
            {"3"}, {"3"}, {"5"}, {"5"}, {null}, {null}, {null}, {null}
        }, "flatJson.clicks BIG_DECIMAL"},

        // ----- nestedJson: multi-level shapes -----
        {"nestedJson", "$.location.country", "STRING", new Object[][]{
            {"US"}, {"US"}, {null}, {null}, {null}, {null}, {null}, {null}
        }, "nestedJson.location.country STRING"},
        {"nestedJson", "$.tags[0]", "STRING", new Object[][]{
            {"green"}, {"green"}, {"red"}, {"red"}, {null}, {null}, {null}, {null}
        }, "nestedJson.tags[0] STRING"},
        {"nestedJson", "$.tags[1]", "STRING", new Object[][]{
            {"blue"}, {"blue"}, {null}, {null}, {null}, {null}, {null}, {null}
        }, "nestedJson.tags[1] STRING (OOB on row 1)"},
        {"nestedJson", "$.events[0].country", "STRING", new Object[][]{
            {"US"}, {"US"}, {null}, {null}, {null}, {null}, {null}, {null}
        }, "nestedJson.events[0].country STRING"}
    };
  }

  /** 1.2 cases — `(column, jsonPath, default SQL literal, expected 8-row result ASC, label)`. */
  @DataProvider(name = "defaultPrecedenceProjectionCases")
  public static Object[][] defaultPrecedenceProjectionCases() {
    return new Object[][]{
        {"flatJson", "$.country", "'foobar'", new Object[][]{
            {"CA"}, {"CA"}, {"US"}, {"US"},
            {"foobar"}, {"foobar"}, {"foobar"}, {"foobar"}
        }, "flatJson.country default 'foobar'"},
        {"nestedJson", "$.location.country", "'foobar'", new Object[][]{
            {"US"}, {"US"},
            {"foobar"}, {"foobar"}, {"foobar"}, {"foobar"}, {"foobar"}, {"foobar"}
        }, "nestedJson.location.country default 'foobar'"}
    };
  }

  // ============================================================================================
  // 2. DISTINCT
  //
  // Pinot rejects positional `ORDER BY 1` for DISTINCT, so each DISTINCT test repeats the
  // projected expression in ORDER BY.
  // ============================================================================================

  /**
   * 2.1 — NH on, 3-arg. The distinct set must include exactly one null entry alongside the
   * resolved values (deduped across segments).
   */
  @Test(dataProvider = "distinctCases")
  public void testDistinctNullHandlingOn(String column, String jsonPath, String resultsType,
      Object[][] expectedRows, String label) {
    String expr = String.format("jsonExtractIndex(%s, '%s', '%s')", column, jsonPath, resultsType);
    String query = String.format("SELECT DISTINCT %s FROM clicks ORDER BY %s ASC NULLS LAST", expr, expr);
    givenCountryClickTable(true).whenQuery(query).thenResultIs(expectedRows);
  }

  /** 2.2 — NH on, 4-arg with non-null default. Default appears as a regular distinct entry (no null). */
  @Test(dataProvider = "defaultPrecedenceDistinctCases")
  public void testDistinctDefaultBeatsNullHandlingPlaceholder(String column, String jsonPath,
      String defaultLiteral, Object[][] expectedRows, String label) {
    String expr = String.format(
        "jsonExtractIndex(%s, '%s', 'STRING', %s)", column, jsonPath, defaultLiteral);
    givenCountryClickTable(true)
        .whenQuery(String.format("SELECT DISTINCT %s FROM clicks ORDER BY %s ASC", expr, expr))
        .thenResultIs(expectedRows);
  }

  /** 2.3 — NH off: DISTINCT throws on unresolved rows. */
  @Test
  public void testDistinctNullHandlingOffThrows() {
    try {
      givenCountryClickTable(false)
          .whenQuery("SELECT DISTINCT jsonExtractIndex(flatJson, '$.country', 'STRING') FROM clicks")
          .thenResultIs(new Object[]{"unused"});
      Assert.fail("Expected DISTINCT to fail when null handling is off and rows are unresolved");
    } catch (AssertionError e) {
      Assertions.assertThat(e.getMessage()).contains("Illegal Json Path");
    }
  }

  /** 2.1 cases — `(column, jsonPath, resultsType, expected distinct rows ASC NULLS LAST, label)`. */
  @DataProvider(name = "distinctCases")
  public static Object[][] distinctCases() {
    return new Object[][]{
        {"flatJson", "$.country", "STRING", new Object[][]{{"CA"}, {"US"}, {null}}, "flatJson.country"},
        {"flatJson", "$.clicks", "INT", new Object[][]{{3}, {5}, {null}}, "flatJson.clicks INT"},
        {"nestedJson", "$.location.country", "STRING", new Object[][]{{"US"}, {null}},
            "nestedJson.location.country"},
        {"nestedJson", "$.tags[0]", "STRING", new Object[][]{{"green"}, {"red"}, {null}},
            "nestedJson.tags[0]"},
        {"nestedJson", "$.events[0].country", "STRING", new Object[][]{{"US"}, {null}},
            "nestedJson.events[0].country"}
    };
  }

  /** 2.2 cases — `(column, jsonPath, default literal, expected distinct rows ASC, label)`. */
  @DataProvider(name = "defaultPrecedenceDistinctCases")
  public static Object[][] defaultPrecedenceDistinctCases() {
    return new Object[][]{
        {"flatJson", "$.country", "'foobar'",
            new Object[][]{{"CA"}, {"US"}, {"foobar"}}, "flatJson.country default 'foobar'"},
        {"nestedJson", "$.location.country", "'foobar'",
            new Object[][]{{"US"}, {"foobar"}}, "nestedJson.location.country default 'foobar'"}
    };
  }

  // ============================================================================================
  // 3. GROUP BY
  // ============================================================================================

  /**
   * 3.1 — NH on, 3-arg. Unresolved rows collapse into a single null group with the correct count,
   * alongside the resolved value groups.
   */
  @Test(dataProvider = "groupByCases")
  public void testGroupByNullHandlingOn(String column, String jsonPath, String resultsType,
      Object[][] expectedRows, String label) {
    String expr = String.format("jsonExtractIndex(%s, '%s', '%s')", column, jsonPath, resultsType);
    givenCountryClickTable(true)
        .whenQuery(String.format(
            "SELECT %s AS v, COUNT(*) FROM clicks GROUP BY v ORDER BY v ASC NULLS LAST", expr))
        .thenResultIs(expectedRows);
  }

  /**
   * 3.2 — NH on, 4-arg with non-null default. Unresolved rows count toward the default's group;
   * no NULL group surfaces.
   */
  @Test(dataProvider = "defaultPrecedenceGroupByCases")
  public void testGroupByDefaultBeatsNullHandlingPlaceholder(String column, String jsonPath,
      String defaultLiteral, Object[][] expectedRows, String label) {
    String expr = String.format(
        "jsonExtractIndex(%s, '%s', 'STRING', %s)", column, jsonPath, defaultLiteral);
    givenCountryClickTable(true)
        .whenQuery(String.format(
            "SELECT %s AS v, COUNT(*) FROM clicks GROUP BY v ORDER BY v ASC", expr))
        .thenResultIs(expectedRows);
  }

  /** 3.3 — NH off: GROUP BY throws on unresolved rows. */
  @Test
  public void testGroupByNullHandlingOffThrows() {
    try {
      givenCountryClickTable(false)
          .whenQuery("SELECT jsonExtractIndex(flatJson, '$.country', 'STRING'), COUNT(*) FROM clicks GROUP BY 1")
          .thenResultIs(new Object[]{"unused"});
      Assert.fail("Expected GROUP BY to fail when null handling is off and rows are unresolved");
    } catch (AssertionError e) {
      Assertions.assertThat(e.getMessage()).contains("Illegal Json Path");
    }
  }

  /** 3.1 cases — `(column, jsonPath, resultsType, expected (value, count) rows ASC NULLS LAST, label)`. */
  @DataProvider(name = "groupByCases")
  public static Object[][] groupByCases() {
    return new Object[][]{
        {"flatJson", "$.country", "STRING", new Object[][]{
            {"CA", 2L}, {"US", 2L}, {null, 4L}
        }, "flatJson.country"},
        {"flatJson", "$.clicks", "INT", new Object[][]{
            {3, 2L}, {5, 2L}, {null, 4L}
        }, "flatJson.clicks INT"},
        {"nestedJson", "$.location.country", "STRING", new Object[][]{
            {"US", 2L}, {null, 6L}
        }, "nestedJson.location.country"},
        {"nestedJson", "$.tags[0]", "STRING", new Object[][]{
            {"green", 2L}, {"red", 2L}, {null, 4L}
        }, "nestedJson.tags[0]"},
        {"nestedJson", "$.events[0].country", "STRING", new Object[][]{
            {"US", 2L}, {null, 6L}
        }, "nestedJson.events[0].country"}
    };
  }

  /** 3.2 cases — `(column, jsonPath, default literal, expected (value, count) rows ASC, label)`. */
  @DataProvider(name = "defaultPrecedenceGroupByCases")
  public static Object[][] defaultPrecedenceGroupByCases() {
    return new Object[][]{
        {"flatJson", "$.country", "'foobar'", new Object[][]{
            {"CA", 2L}, {"US", 2L}, {"foobar", 4L}
        }, "flatJson.country default 'foobar'"},
        {"nestedJson", "$.location.country", "'foobar'", new Object[][]{
            {"US", 2L}, {"foobar", 6L}
        }, "nestedJson.location.country default 'foobar'"}
    };
  }
}
