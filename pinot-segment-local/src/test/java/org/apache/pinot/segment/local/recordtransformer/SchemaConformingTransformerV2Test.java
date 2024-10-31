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
package org.apache.pinot.segment.local.recordtransformer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerV2Config;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;


public class SchemaConformingTransformerV2Test {
  private static final String INDEXABLE_EXTRAS_FIELD_NAME = "json_data";
  private static final String UNINDEXABLE_EXTRAS_FIELD_NAME = "json_data_no_idx";
  private static final String UNINDEXABLE_FIELD_SUFFIX = "_noIndex";
  private static final String MERGED_TEXT_INDEX_FIELD_NAME = "__mergedTextIndex";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonNodeFactory N = OBJECT_MAPPER.getNodeFactory();
  private static final String TEST_JSON_ARRAY_FIELD_NAME = "arrayField";
  private static final String TEST_JSON_NULL_FIELD_NAME = "nullField";
  private static final String TEST_JSON_STRING_FIELD_NAME = "stringField";
  private static final String TEST_JSON_MAP_FIELD_NAME = "mapField";
  private static final String TEST_JSON_MAP_EXTRA_FIELD_NAME = "mapFieldExtra";
  private static final String TEST_JSON_MAP_NO_IDX_FIELD_NAME = "mapField_noIndex";
  private static final String TEST_JSON_NESTED_MAP_FIELD_NAME = "nestedFields";
  private static final String TEST_JSON_INT_NO_IDX_FIELD_NAME = "intField_noIndex";
  private static final String TEST_JSON_STRING_NO_IDX_FIELD_NAME = "stringField_noIndex";
  private static final ArrayNode TEST_JSON_ARRAY_NODE = N.arrayNode().add(0).add(1).add(2).add(3);
  private static final NullNode TEST_JSON_NULL_NODE = N.nullNode();
  private static final TextNode TEST_JSON_STRING_NODE = N.textNode("a");
  private static final NumericNode TEST_INT_NODE = N.numberNode(9);
  private static final TextNode TEST_JSON_STRING_NO_IDX_NODE = N.textNode("z");
  private static final CustomObjectNode TEST_JSON_MAP_NODE =
      CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
          .set(TEST_JSON_NULL_FIELD_NAME, TEST_JSON_NULL_NODE).set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE);
  private static final CustomObjectNode TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD =
      CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
          .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE);

  private static final CustomObjectNode TEST_JSON_MAP_NO_IDX_NODE =
      CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
          .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE);
  private static final CustomObjectNode TEST_JSON_MAP_NODE_WITH_NO_IDX =
      CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
          .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE).set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
          .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE);

  static {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  private static final SchemaConformingTransformerV2 _RECORD_TRANSFORMER =
      new SchemaConformingTransformerV2(createDefaultBasicTableConfig(), createDefaultSchema());

  private static TableConfig createDefaultBasicTableConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    SchemaConformingTransformerV2Config schemaConformingTransformerV2Config =
        new SchemaConformingTransformerV2Config(true, INDEXABLE_EXTRAS_FIELD_NAME, true, UNINDEXABLE_EXTRAS_FIELD_NAME,
            UNINDEXABLE_FIELD_SUFFIX, null, null, null, null, null, null, null, null, null);
    ingestionConfig.setSchemaConformingTransformerV2Config(schemaConformingTransformerV2Config);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig)
        .build();
  }

  private static TableConfig createDefaultTableConfig(String indexableExtrasField, String unindexableExtrasField,
      String unindexableFieldSuffix, Set<String> fieldPathsToDrop, Set<String> fieldPathsToPreserve,
      Set<String> fieldPathToPreserverWithIndex, String mergedTextIndexField) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    SchemaConformingTransformerV2Config schemaConformingTransformerV2Config =
        new SchemaConformingTransformerV2Config(indexableExtrasField != null, indexableExtrasField,
            unindexableExtrasField != null, unindexableExtrasField, unindexableFieldSuffix, fieldPathsToDrop,
            fieldPathsToPreserve, fieldPathToPreserverWithIndex, mergedTextIndexField, null, null, null, null, null);
    ingestionConfig.setSchemaConformingTransformerV2Config(schemaConformingTransformerV2Config);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig)
        .build();
  }

  private static Schema createDefaultSchema() {
    return createDefaultSchemaBuilder().addSingleValueDimension("intField", DataType.INT).build();
  }

  private static Schema.SchemaBuilder createDefaultSchemaBuilder() {
    return new Schema.SchemaBuilder().addSingleValueDimension(INDEXABLE_EXTRAS_FIELD_NAME, DataType.JSON)
        .addSingleValueDimension(UNINDEXABLE_EXTRAS_FIELD_NAME, DataType.JSON);
  }

  @Test
  public void testWithNoUnindexableFields() {
    /*
    {
      "arrayField" : [ 0, 1, 2, 3 ],
      "stringField" : "a",
      "mapField" : {
        "arrayField" : [ 0, 1, 2, 3 ],
        "stringField" : "a"
      },
      "nestedField" : {
        "arrayField" : [ 0, 1, 2, 3 ],
        "stringField" : "a",
        "mapField" : {
          "arrayField" : [ 0, 1, 2, 3 ],
          "stringField" : "a"
        }
      }
    }
    */
    final CustomObjectNode inputJsonNode =
        CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE).set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE)
            .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE).set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE));

    CustomObjectNode expectedJsonNode;
    Schema schema;

    // No dedicated columns, everything moved under INDEXABLE_EXTRAS_FIELD_NAME
    /*
    {
      "json_data" : {
        "arrayField" : [ 0, 1, 2, 3 ],
        "stringField" : "a",
        "mapField" : {
          "arrayField" : [ 0, 1, 2, 3 ],
          "stringField" : "a"
        },
        "nestedField" : {
          "arrayField" : [ 0, 1, 2, 3 ],
          "stringField" : "a",
          "mapField" : {
            "arrayField" : [ 0, 1, 2, 3 ],
            "stringField" : "a"
          }
        }
      }
    }
    */
    schema = createDefaultSchemaBuilder().build();
    // The input json node stripped of null fields.
    final CustomObjectNode inputJsonNodeWithoutNullFields =
        CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
            .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD).set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                    .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD));

    expectedJsonNode = CustomObjectNode.create().set(INDEXABLE_EXTRAS_FIELD_NAME, inputJsonNodeWithoutNullFields);
    transformWithIndexableFields(schema, inputJsonNode, expectedJsonNode);

    // Three dedicated columns in schema, only two are populated, one ignored
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nestedFields.stringField":"a",
      "<indexableExtras>":{
        "mapField": {
          "arrayField":[0, 1, 2, 3],
          "stringField":"a"
        },
        "stringField":"a",
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "stringField":"a"
          }
        }
      }
    }
    */
    schema = createDefaultSchemaBuilder().addMultiValueDimension(TEST_JSON_ARRAY_FIELD_NAME, DataType.INT)
        .addSingleValueDimension(TEST_JSON_MAP_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .build();
    expectedJsonNode = CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .setAll(TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD.deepCopy().removeAndReturn(TEST_JSON_ARRAY_FIELD_NAME))
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME, CustomObjectNode.create().setAll(
                        TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD.deepCopy().removeAndReturn(TEST_JSON_STRING_FIELD_NAME))
                    .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)));
    transformWithIndexableFields(schema, inputJsonNode, expectedJsonNode);

    // 8 dedicated columns, only 6 are populated
    /*
    {
      "arrayField" : [ 0, 1, 2, 3 ],
      "stringField" : "a",
      "nestedField.arrayField" : [ 0, 1, 2, 3 ],
      "nestedField.stringField" : "a",
      "json_data" : {
        "mapField" : {
          "arrayField" : [ 0, 1, 2, 3 ],
          "stringField" : "a"
        },
        "nestedField" : {
          "mapField" : {
            "arrayField" : [ 0, 1, 2, 3 ],
            "stringField" : "a"
          }
        }
      }
    }
    */
    schema = createDefaultSchemaBuilder().addMultiValueDimension(TEST_JSON_ARRAY_FIELD_NAME, DataType.INT)
        .addSingleValueDimension(TEST_JSON_NULL_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_MAP_FIELD_NAME, DataType.JSON)
        .addMultiValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, DataType.INT)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_NULL_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_MAP_FIELD_NAME, DataType.JSON)
        .build();
    expectedJsonNode = CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)));
    transformWithIndexableFields(schema, inputJsonNode, expectedJsonNode);
  }

  @Test
  public void testWithUnindexableFieldsAndMergedTextIndex() {
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "stringField":"a",
      "intField_noIndex":9,
      "string_noIndex":"z",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z"
      },
      "mapField_noIndex":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
      },
      "nestedFields":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a",
          "intField_noIndex":9,
          "string_noIndex":"z"
        }
      }
    }
    */
    final CustomObjectNode inputJsonNode =
        CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE).set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
            .set(TEST_JSON_NULL_FIELD_NAME, TEST_JSON_NULL_NODE).set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
            .set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
            .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
            .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX)
            .set(TEST_JSON_MAP_NO_IDX_FIELD_NAME, TEST_JSON_MAP_NODE).set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE).set(TEST_JSON_ARRAY_FIELD_NAME,
                        TEST_JSON_ARRAY_NODE)
                    .set(TEST_JSON_NULL_FIELD_NAME, TEST_JSON_NULL_NODE)
                    .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
                    .set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                    .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                    .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX));

    CustomObjectNode expectedJsonNode;
    CustomObjectNode expectedJsonNodeWithMergedTextIndex;
    Schema.SchemaBuilder schemaBuilder;

    // No schema
    schemaBuilder = createDefaultSchemaBuilder();
    /* Expected output
    {
      "indexableExtras":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a"
        },
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a",
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "stringField":"a"
          }
        }
      },
      "unindexableExtras":{
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField":{
          "intField_noIndex":9,
          "string_noIndex":"z"
        },
        "mapField_noIndex":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a",
        },
        "nestedFields":{
          "intField_noIndex":9,
          "string_noIndex":"z",
          "mapField":{
            "intField_noIndex":9,
            "string_noIndex":"z"
          }
        }
      },
      __mergedTextIndex: [
        // See the value of expectedJsonNodeWithMergedTextIndex
      ]
    }
    */
    expectedJsonNode = CustomObjectNode.create().set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
                .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
                .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
                        .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
                        .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)))
        .set(UNINDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NO_IDX_NODE)
                .set(TEST_JSON_MAP_NO_IDX_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                        .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                        .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NO_IDX_NODE)));
    transformWithUnIndexableFieldsAndMergedTextIndex(schemaBuilder.build(), inputJsonNode, expectedJsonNode);

    expectedJsonNodeWithMergedTextIndex = expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME,
        N.arrayNode().add("[0,1,2,3]:arrayField").add("0:arrayField").add("1:arrayField").add("2:arrayField")
            .add("3:arrayField").add("a:stringField").add("[0,1,2,3]:mapField.arrayField").add("0:mapField.arrayField")
            .add("1:mapField.arrayField").add("2:mapField.arrayField").add("3:mapField.arrayField")
            .add("a:mapField.stringField").add("[0,1,2,3]:nestedFields.arrayField").add("0:nestedFields.arrayField")
            .add("1:nestedFields.arrayField").add("2:nestedFields.arrayField").add("3:nestedFields.arrayField")
            .add("a:nestedFields.stringField").add("[0,1,2,3]:nestedFields.mapField.arrayField")
            .add("0:nestedFields.mapField.arrayField").add("1:nestedFields.mapField.arrayField")
            .add("2:nestedFields.mapField.arrayField").add("3:nestedFields.mapField.arrayField")
            .add("a:nestedFields.mapField.stringField"));
    transformWithUnIndexableFieldsAndMergedTextIndex(
        schemaBuilder.addMultiValueDimension(MERGED_TEXT_INDEX_FIELD_NAME, DataType.STRING).build(), inputJsonNode,
        expectedJsonNodeWithMergedTextIndex);

    // With schema, mapField is not indexed
    schemaBuilder = createDefaultSchemaBuilder().addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension(TEST_JSON_MAP_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME, DataType.JSON)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, DataType.STRING);
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nestedFields.stringField":"a",
      "indexableExtras":{
        "stringField":"a",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a"
        },
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "stringField":"a"
          }
        }
      },
      "unindexableExtras":{
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField":{
          "intField_noIndex":9,
          "string_noIndex":"z"
        },
        "mapField_noIndex":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a",
        },
        "nestedFields":{
          "intField_noIndex":9,
          "string_noIndex":"z",
          "mapField":{
            "intField_noIndex":9,
            "string_noIndex":"z"
          }
        }
      },
      __mergedTextIndex: [
        // See the value of expectedJsonNodeWithMergedTextIndex
      ]
    }
    */
    expectedJsonNode = CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
                .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
                        .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)))

        .set(UNINDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NO_IDX_NODE)
                .set(TEST_JSON_MAP_NO_IDX_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                        .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                        .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NO_IDX_NODE)));
    transformWithUnIndexableFieldsAndMergedTextIndex(schemaBuilder.build(), inputJsonNode, expectedJsonNode);

    expectedJsonNodeWithMergedTextIndex = expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME,
        N.arrayNode().add("[0,1,2,3]:arrayField").add("0:arrayField").add("1:arrayField").add("2:arrayField")
            .add("3:arrayField").add("a:stringField").add("[0,1,2,3]:mapField.arrayField").add("0:mapField.arrayField")
            .add("1:mapField.arrayField").add("2:mapField.arrayField").add("3:mapField.arrayField")
            .add("a:mapField.stringField").add("[0,1,2,3]:nestedFields.arrayField").add("0:nestedFields.arrayField")
            .add("1:nestedFields.arrayField").add("2:nestedFields.arrayField").add("3:nestedFields.arrayField")
            .add("a:nestedFields.stringField").add("[0,1,2,3]:nestedFields.mapField.arrayField")
            .add("0:nestedFields.mapField.arrayField").add("1:nestedFields.mapField.arrayField")
            .add("2:nestedFields.mapField.arrayField").add("3:nestedFields.mapField.arrayField")
            .add("a:nestedFields.mapField.stringField"));
    transformWithUnIndexableFieldsAndMergedTextIndex(
        schemaBuilder.addMultiValueDimension(MERGED_TEXT_INDEX_FIELD_NAME, DataType.STRING).build(), inputJsonNode,
        expectedJsonNodeWithMergedTextIndex);

    // With all fields in schema, but map field would not be indexed
    schemaBuilder = createDefaultSchemaBuilder().addMultiValueDimension(TEST_JSON_ARRAY_FIELD_NAME, DataType.INT)
        .addSingleValueDimension(TEST_JSON_NULL_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_MAP_FIELD_NAME, DataType.JSON)
        .addMultiValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, DataType.INT)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_NULL_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_MAP_FIELD_NAME, DataType.JSON);
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "stringField":"a",
      "nestedFields.arrayField":[0, 1, 2, 3],
      "nestedFields.stringField":"a",
      "indexableExtras":{
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a"
        },
        "nestedFields":{
          mapField":{
            "arrayField":[0, 1, 2, 3],
            "stringField":"a"
          }
        }
      },
      "unindexableExtras":{
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField":{
          "intField_noIndex":9,
          "string_noIndex":"z"
        },
        "mapField_noIndex":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a",
        },
        "nestedFields":{
          "intField_noIndex":9,
          "string_noIndex":"z",
          "mapField":{
            "intField_noIndex":9,
            "string_noIndex":"z"
          }
        }
      },
      __mergedTextIndex: [
        // See the value of expectedJsonNodeWithMergedTextIndex
      ]
    }
    */
    expectedJsonNode = CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
        .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)))

        .set(UNINDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NO_IDX_NODE)
                .set(TEST_JSON_MAP_NO_IDX_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                        .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                        .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NO_IDX_NODE)));
    transformWithUnIndexableFieldsAndMergedTextIndex(schemaBuilder.build(), inputJsonNode, expectedJsonNode);
    expectedJsonNodeWithMergedTextIndex = expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME,
        N.arrayNode().add("[0,1,2,3]:arrayField").add("0:arrayField").add("1:arrayField").add("2:arrayField")
            .add("3:arrayField").add("a:stringField").add("[0,1,2,3]:mapField.arrayField").add("0:mapField.arrayField")
            .add("1:mapField.arrayField").add("2:mapField.arrayField").add("3:mapField.arrayField")
            .add("a:mapField.stringField").add("[0,1,2,3]:nestedFields.arrayField").add("0:nestedFields.arrayField")
            .add("1:nestedFields.arrayField").add("2:nestedFields.arrayField").add("3:nestedFields.arrayField")
            .add("a:nestedFields.stringField").add("[0,1,2,3]:nestedFields.mapField.arrayField")
            .add("0:nestedFields.mapField.arrayField").add("1:nestedFields.mapField.arrayField")
            .add("2:nestedFields.mapField.arrayField").add("3:nestedFields.mapField.arrayField")
            .add("a:nestedFields.mapField.stringField"));
    transformWithUnIndexableFieldsAndMergedTextIndex(
        schemaBuilder.addMultiValueDimension(MERGED_TEXT_INDEX_FIELD_NAME, DataType.STRING).build(), inputJsonNode,
        expectedJsonNodeWithMergedTextIndex);
  }

  @Test
  public void testKeyValueTransformation() {
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "stringField":"a",
      "intField_noIndex":9,
      "string_noIndex":"z",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z"
      },
      "mapFieldExtra":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z"
      },
      "mapField_noIndex":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
      },
      "nestedFields":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a",
          "intField_noIndex":9,
          "string_noIndex":"z"
        }
      }
    }
    */
    final CustomObjectNode inputJsonNode =
        CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE).set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
            .set(TEST_JSON_NULL_FIELD_NAME, TEST_JSON_NULL_NODE).set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
            .set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
            .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
            .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX)
            .set(TEST_JSON_MAP_EXTRA_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX)
            .set(TEST_JSON_MAP_NO_IDX_FIELD_NAME, TEST_JSON_MAP_NODE).set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE).set(TEST_JSON_ARRAY_FIELD_NAME,
                        TEST_JSON_ARRAY_NODE)
                    .set(TEST_JSON_NULL_FIELD_NAME, TEST_JSON_NULL_NODE)
                    .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
                    .set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                    .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                    .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX));

    CustomObjectNode expectedJsonNode;
    CustomObjectNode expectedJsonNodeWithMergedTextIndex;
    Schema.SchemaBuilder schemaBuilder;

    String destColumnName = "someMeaningfulName";
    // make array field as single value STRING, test the conversion function
    // ignore the column nestedFields
    // preserve the entire mapField value
    // map the column someMeaningfulName to nestedFields.stringField
    schemaBuilder = createDefaultSchemaBuilder().addSingleValueDimension("arrayField", DataType.STRING)
        .addSingleValueDimension(TEST_JSON_MAP_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_MAP_EXTRA_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME, DataType.JSON)
        .addSingleValueDimension(destColumnName, DataType.STRING);

    Map<String, String> keyMapping = new HashMap<>() {
      {
        put(destColumnName, TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME);
      }
    };
    Set<String> pathToDrop = new HashSet<>() {
      {
        add(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_MAP_FIELD_NAME);
      }
    };
    Set<String> pathToPreserve = new HashSet<>() {
      {
        add(TEST_JSON_MAP_FIELD_NAME);
      }
    };
    Set<String> pathToPreserveWithIndex = new HashSet<>() {
      {
        add(TEST_JSON_MAP_EXTRA_FIELD_NAME);
      }
    };

    /*
    {
      "arrayField":[0,1,2,3],
      "nestedFields.stringField":"a",
      "mapField":{
        "arrayField":[0,1,2,3],
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z"
      },
      "mapFieldExtra":{
        "arrayField":[0,1,2,3],
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z"
      }
      "indexableExtras":{
        "stringField":"a",
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
        }
      },
      "unindexableExtras":{
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField_noIndex":{
          "arrayField":[0, 1, 2, 3],
          "stringField":"a",
        },
        "nestedFields":{
          "intField_noIndex":9,
          "string_noIndex":"z"
        }
      },
      __mergedTextIndex: [
        // check expectedJsonNodeWithMergedTextIndex
      ]
    }
    */
    expectedJsonNode = CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, N.textNode("[0,1,2,3]"))
        .set(destColumnName, TEST_JSON_STRING_NODE).set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX)
        .set(TEST_JSON_MAP_EXTRA_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX).set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)))

        .set(UNINDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                .set(TEST_JSON_MAP_NO_IDX_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                        .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)));

    expectedJsonNodeWithMergedTextIndex = expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME,
        N.arrayNode().add("0:arrayField").add("1:arrayField").add("2:arrayField").add("3:arrayField")
            .add("[0,1,2,3]:arrayField").add("a:stringField").add("[0,1,2,3]:nestedFields.arrayField")
            .add("0:nestedFields.arrayField").add("1:nestedFields.arrayField").add("2:nestedFields.arrayField")
            .add("3:nestedFields.arrayField").add("a:nestedFields.stringField")
            .add("[0,1,2,3]:mapFieldExtra.arrayField").add("a:mapFieldExtra.stringField")
            .add("0:mapFieldExtra.arrayField").add("1:mapFieldExtra.arrayField").add("2:mapFieldExtra.arrayField")
            .add("3:mapFieldExtra.arrayField"));
    transformKeyValueTransformation(
        schemaBuilder.addMultiValueDimension(MERGED_TEXT_INDEX_FIELD_NAME, DataType.STRING).build(), keyMapping,
        pathToDrop, pathToPreserve, pathToPreserveWithIndex, inputJsonNode, expectedJsonNodeWithMergedTextIndex);
  }

  private void transformWithIndexableFields(Schema schema, JsonNode inputRecordJsonNode, JsonNode ouputRecordJsonNode) {
    testTransform(INDEXABLE_EXTRAS_FIELD_NAME, null, null, schema, null, null, null, null,
        inputRecordJsonNode.toString(), ouputRecordJsonNode.toString());
  }

  private void transformWithUnIndexableFieldsAndMergedTextIndex(Schema schema, JsonNode inputRecordJsonNode,
      JsonNode ouputRecordJsonNode) {
    testTransform(INDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_EXTRAS_FIELD_NAME, MERGED_TEXT_INDEX_FIELD_NAME, schema,
        null, null, null, null, inputRecordJsonNode.toString(), ouputRecordJsonNode.toString());
  }

  private void transformKeyValueTransformation(Schema schema, Map<String, String> keyMapping,
      Set<String> fieldPathsToDrop, Set<String> fieldPathsToPreserve, Set<String> fieldPathsToPreserveWithIndex,
      JsonNode inputRecordJsonNode, JsonNode ouputRecordJsonNode) {
    testTransform(INDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_EXTRAS_FIELD_NAME, MERGED_TEXT_INDEX_FIELD_NAME, schema,
        keyMapping, fieldPathsToDrop, fieldPathsToPreserve, fieldPathsToPreserveWithIndex,
        inputRecordJsonNode.toString(), ouputRecordJsonNode.toString());
  }

  private void testTransform(String indexableExtrasField, String unindexableExtrasField, String mergedTextIndexField,
      Schema schema, Map<String, String> keyMapping, Set<String> fieldPathsToDrop, Set<String> fieldPathsToPreserve,
      Set<String> fieldPathsToPreserveWithIndex, String inputRecordJSONString, String expectedOutputRecordJSONString) {
    TableConfig tableConfig =
        createDefaultTableConfig(indexableExtrasField, unindexableExtrasField, UNINDEXABLE_FIELD_SUFFIX,
            fieldPathsToDrop, fieldPathsToPreserve, fieldPathsToPreserveWithIndex, mergedTextIndexField);
    tableConfig.getIngestionConfig().getSchemaConformingTransformerV2Config().setColumnNameToJsonKeyPathMap(keyMapping);
    GenericRow outputRecord = transformRow(tableConfig, schema, inputRecordJSONString);
    Map<String, Object> expectedOutputRecordMap = jsonStringToMap(expectedOutputRecordJSONString);

    // Merged text index field does not need to have deterministic order
    Object mergedTextIndexValue = outputRecord.getFieldToValueMap().get(MERGED_TEXT_INDEX_FIELD_NAME);
    Object expectedMergedTextIndexValue = expectedOutputRecordMap.get(MERGED_TEXT_INDEX_FIELD_NAME);
    if (mergedTextIndexValue != null) {
      ((List<Object>) mergedTextIndexValue).sort(null);
    }
    if (expectedMergedTextIndexValue != null) {
      ((List<Object>) expectedMergedTextIndexValue).sort(null);
    }

    Assert.assertNotNull(outputRecord);
    Assert.assertEquals(outputRecord.getFieldToValueMap(), expectedOutputRecordMap);
  }

  /**
   * Transforms the given row (given as a JSON string) using the transformer
   * @return The transformed row
   */
  private GenericRow transformRow(TableConfig tableConfig, Schema schema, String inputRecordJSONString) {
    Map<String, Object> inputRecordMap = jsonStringToMap(inputRecordJSONString);
    GenericRow inputRecord = createRowFromMap(inputRecordMap);
    SchemaConformingTransformerV2 schemaConformingTransformerV2 =
        new SchemaConformingTransformerV2(tableConfig, schema);
    return schemaConformingTransformerV2.transform(inputRecord);
  }

  /**
   * @return A map representing the given JSON string
   */
  @Nonnull
  private Map<String, Object> jsonStringToMap(String jsonString) {
    try {
      TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {
      };
      return OBJECT_MAPPER.readValue(jsonString, typeRef);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    // Should never reach here
    return null;
  }

  /**
   * @return A new generic row with all the kv-pairs from the given map
   */
  private GenericRow createRowFromMap(Map<String, Object> map) {
    GenericRow record = new GenericRow();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      record.putValue(entry.getKey(), entry.getValue());
    }
    return record;
  }

  @Test
  public void testOverlappingSchemaFields() {
    try {
      Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("a.b", DataType.STRING)
          .addSingleValueDimension("a.b.c", DataType.INT).build();
      SchemaConformingTransformerV2.validateSchema(schema,
          new SchemaConformingTransformerV2Config(null, INDEXABLE_EXTRAS_FIELD_NAME, null, null, null, null, null, null,
              null, null, null, null, null, null));
    } catch (Exception ex) {
      fail("Should not have thrown any exception when overlapping schema occurs");
    }

    try {
      // This is a repeat of the previous test but with fields reversed just in case they are processed in order
      Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("a.b.c", DataType.INT)
          .addSingleValueDimension("a.b", DataType.STRING).build();
      SchemaConformingTransformerV2.validateSchema(schema,
          new SchemaConformingTransformerV2Config(null, INDEXABLE_EXTRAS_FIELD_NAME, null, null, null, null, null, null,
              null, null, null, null, null, null));
    } catch (Exception ex) {
      fail("Should not have thrown any exception when overlapping schema occurs");
    }
  }

  @Test
  public void testBase64ValueFilter() {
    String text = "Hello world";
    String binaryData = "ABCxyz12345-_+/=";
    String binaryDataWithTrailingPeriods = "ABCxyz12345-_+/=..";
    String binaryDataWithRandomPeriods = "A.BCxy.z12345-_+/=..";
    String shortBinaryData = "short";
    int minLength = 10;

    assertFalse(_RECORD_TRANSFORMER.base64ValueFilter(text.getBytes(), minLength));
    assertTrue(_RECORD_TRANSFORMER.base64ValueFilter(binaryData.getBytes(), minLength));
    assertTrue(_RECORD_TRANSFORMER.base64ValueFilter(binaryDataWithTrailingPeriods.getBytes(), minLength));
    assertFalse(_RECORD_TRANSFORMER.base64ValueFilter(binaryDataWithRandomPeriods.getBytes(), minLength));
    assertFalse(_RECORD_TRANSFORMER.base64ValueFilter(shortBinaryData.getBytes(), minLength));
  }

  @Test
  public void testShingleIndexTokenization() {
    String key = "key";
    String value = "0123456789ABCDEFGHIJ";
    int shingleIndexMaxLength;
    int shingleIndexOverlapLength;
    List<String> expectedTokenValues;

    shingleIndexMaxLength = 8;
    shingleIndexOverlapLength = 1;
    expectedTokenValues = new ArrayList<>(
        Arrays.asList("0123:key", "3456:key", "6789:key", "9ABC:key", "CDEF:key", "FGHI:key", "IJ:key"));
    testShingleIndexWithParams(key, value, shingleIndexMaxLength, shingleIndexOverlapLength, expectedTokenValues);

    shingleIndexMaxLength = 8;
    shingleIndexOverlapLength = 2;
    expectedTokenValues = new ArrayList<>(
        Arrays.asList("0123:key", "2345:key", "4567:key", "6789:key", "89AB:key", "ABCD:key", "CDEF:key", "EFGH:key",
            "GHIJ:key"));
    testShingleIndexWithParams(key, value, shingleIndexMaxLength, shingleIndexOverlapLength, expectedTokenValues);

    // If shingleIndexMaxLength is lower than the minimum required length for merged text index token
    // (length of the key + shingling overlap length + 1), then the shingleIndexMaxLength is adjusted to
    // the maximum Lucene token size (32766)
    shingleIndexMaxLength = 1;
    shingleIndexOverlapLength = 5;
    expectedTokenValues = new ArrayList<>(Arrays.asList(value + ":" + key));
    testShingleIndexWithParams(key, value, shingleIndexMaxLength, shingleIndexOverlapLength, expectedTokenValues);

    // If shingleIndexOverlapLength is equal to or longer than the length of the value, shingling cannot be applied and
    // only one token is generated.
    shingleIndexMaxLength = 32766;
    shingleIndexOverlapLength = 100;
    expectedTokenValues = new ArrayList<>(Arrays.asList(value + ":" + key));
    testShingleIndexWithParams(key, value, shingleIndexMaxLength, shingleIndexOverlapLength, expectedTokenValues);

    // Other corner cases, where the result would be the same as if shingling has not been applied
    shingleIndexMaxLength = 300;
    shingleIndexOverlapLength = 10;
    expectedTokenValues = new ArrayList<>(Arrays.asList(value + ":" + key));
    testShingleIndexWithParams(key, value, shingleIndexMaxLength, shingleIndexOverlapLength, expectedTokenValues);
  }

  private void testShingleIndexWithParams(String key, String value, Integer shingleIndexMaxLength,
      Integer shingleIndexOverlapLength, List<String> expectedTokenValues) {
    Map.Entry<String, Object> kv = new AbstractMap.SimpleEntry<>(key, value);
    List<String> shingleIndexTokens = new ArrayList<>();
    _RECORD_TRANSFORMER.generateShingleTextIndexDocument(kv, shingleIndexTokens, shingleIndexMaxLength,
        shingleIndexOverlapLength);
    int numTokens = shingleIndexTokens.size();
    assertEquals(numTokens, expectedTokenValues.size());
    for (int i = 0; i < numTokens; i++) {
      assertEquals(shingleIndexTokens.get(i), expectedTokenValues.get(i));
    }
  }

  static class CustomObjectNode extends ObjectNode {
    public CustomObjectNode() {
      super(OBJECT_MAPPER.getNodeFactory());
    }

    public static CustomObjectNode create() {
      return new CustomObjectNode();
    }

    public CustomObjectNode set(String fieldName, JsonNode value) {
      super.set(fieldName, value);
      return this;
    }

    public CustomObjectNode setAll(ObjectNode other) {
      super.setAll(other);
      return this;
    }

    public CustomObjectNode removeAndReturn(String fieldName) {
      super.remove(fieldName);
      return this;
    }

    public CustomObjectNode deepCopy() {
      return CustomObjectNode.create().setAll(this);
    }
  }

  static {
    ServerMetrics.register(mock(ServerMetrics.class));
  }
}
