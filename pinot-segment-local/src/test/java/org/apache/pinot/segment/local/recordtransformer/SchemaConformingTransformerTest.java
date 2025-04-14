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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;


public class SchemaConformingTransformerTest {
  private static final String INDEXABLE_EXTRAS_FIELD_NAME = "json_data";
  private static final String UNINDEXABLE_EXTRAS_FIELD_NAME = "json_data_no_idx";
  private static final String UNINDEXABLE_FIELD_SUFFIX = "_noIndex";
  private static final String MERGED_TEXT_INDEX_FIELD_NAME = "__mergedTextIndex";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonNodeFactory N = OBJECT_MAPPER.getNodeFactory();
  private static final String TEST_JSON_MESSAGE_NAME = "message";
  private static final String TEST_JSON_MESSAGE_LOGTYPE_NAME = "message_logtype";
  private static final String TEST_JSON_ARRAY_FIELD_NAME = "arrayField";
  private static final String TEST_JSON_NULL_FIELD_NAME = "nullField";
  private static final String TEST_JSON_STRING_FIELD_NAME = "stringField";
  private static final String TEST_JSON_DOT_FIELD_NAME = "dotField.dotSuffix";
  private static final String TEST_JSON_MAP_FIELD_NAME = "mapField";
  private static final String TEST_JSON_MAP_EXTRA_FIELD_NAME = "mapFieldExtra";
  private static final String TEST_JSON_MAP_NO_IDX_FIELD_NAME = "mapField_noIndex";
  private static final String TEST_JSON_NESTED_MAP_FIELD_NAME = "nestedFields";
  private static final String TEST_JSON_INT_NO_IDX_FIELD_NAME = "intField_noIndex";
  private static final String TEST_JSON_STRING_NO_IDX_FIELD_NAME = "stringField_noIndex";
  private static final ArrayNode TEST_JSON_ARRAY_NODE = N.arrayNode().add(0).add(1).add(2).add(3);
  private static final NullNode TEST_JSON_NULL_NODE = N.nullNode();
  private static final TextNode TEST_JSON_STRING_NODE = N.textNode("a");
  private static final TextNode TEST_JSON_STRING_NODE_WITH_UPEERCASE = N.textNode("aA_123");
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
  private static final String JSON_KEY_VALUE_SEPARATOR = "\u001e";
  private static final String MERGED_TEXT_INDEX_BOD_ANCHOR = "\u0002";
  private static final String MERGED_TEXT_INDEX_EOD_ANCHOR = "\u0003";

  static {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  private static final SchemaConformingTransformer _RECORD_TRANSFORMER =
      new SchemaConformingTransformer(createDefaultBasicTableConfig(), createDefaultSchema());

  private static TableConfig createDefaultBasicTableConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    SchemaConformingTransformerConfig schemaConformingTransformerConfig =
        new SchemaConformingTransformerConfig(true, INDEXABLE_EXTRAS_FIELD_NAME, true, UNINDEXABLE_EXTRAS_FIELD_NAME,
            UNINDEXABLE_FIELD_SUFFIX, null, null, null, null, null, null, false, null, null, null, null, null, null,
            null, null, null, null);
    ingestionConfig.setSchemaConformingTransformerConfig(schemaConformingTransformerConfig);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig)
        .build();
  }

  private static TableConfig createDefaultTableConfig(String indexableExtrasField, String unindexableExtrasField,
      String unindexableFieldSuffix, Set<String> fieldPathsToDrop, Set<String> fieldPathsToPreserve,
      Set<String> fieldPathsToPreserveWithIndex, Map<String, String> columnNameToJsonKeyPathMap,
      String mergedTextIndexField, boolean useAnonymousDotInFieldNames, boolean optimizeCaseInsensitiveSearch,
      Boolean reverseTextIndexKeyValueOrder) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    SchemaConformingTransformerConfig schemaConformingTransformerConfig =
        new SchemaConformingTransformerConfig(indexableExtrasField != null, indexableExtrasField,
            unindexableExtrasField != null, unindexableExtrasField, unindexableFieldSuffix, fieldPathsToDrop,
            fieldPathsToPreserve, fieldPathsToPreserveWithIndex, null, columnNameToJsonKeyPathMap,
            mergedTextIndexField, useAnonymousDotInFieldNames, optimizeCaseInsensitiveSearch,
            reverseTextIndexKeyValueOrder, null, null, null,
            null, null, JSON_KEY_VALUE_SEPARATOR, MERGED_TEXT_INDEX_BOD_ANCHOR, MERGED_TEXT_INDEX_EOD_ANCHOR);
    ingestionConfig.setSchemaConformingTransformerConfig(schemaConformingTransformerConfig);
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
      "dotField.dotSuffix" : "a",
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
            .set(TEST_JSON_DOT_FIELD_NAME, TEST_JSON_STRING_NODE)
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
        "dotField.dotSuffix" : "a",
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
            .set(TEST_JSON_DOT_FIELD_NAME, TEST_JSON_STRING_NODE)
            .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD).set(TEST_JSON_NESTED_MAP_FIELD_NAME,
            CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD));

    expectedJsonNode = CustomObjectNode.create().set(INDEXABLE_EXTRAS_FIELD_NAME, inputJsonNodeWithoutNullFields);
    transformWithIndexableFields(schema, inputJsonNode, expectedJsonNode, true);

    // Four dedicated columns in schema, only two are populated, two ignored
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nestedFields.stringField":"a",
      "<indexableExtras>":{
        "dotField.dotSuffix" : "a", // it is not loaded to dedicated column because we do not enable anonymous dot in
         field names
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
        .addSingleValueDimension(TEST_JSON_DOT_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .build();
    expectedJsonNode = CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .setAll(TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD.deepCopy().removeAndReturn(TEST_JSON_ARRAY_FIELD_NAME))
                .set(TEST_JSON_DOT_FIELD_NAME, TEST_JSON_STRING_NODE)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME, CustomObjectNode.create().setAll(
                    TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD.deepCopy().removeAndReturn(TEST_JSON_STRING_FIELD_NAME))
                    .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)));
    transformWithIndexableFields(schema, inputJsonNode, expectedJsonNode, false);

    // 8 dedicated columns, only 6 are populated
    /*
    {
      "arrayField" : [ 0, 1, 2, 3 ],
      "stringField" : "a",
      "dotField.dotSuffix" : "a",
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
        .addSingleValueDimension(TEST_JSON_DOT_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_MAP_FIELD_NAME, DataType.JSON)
        .addMultiValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, DataType.INT)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_NULL_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_MAP_FIELD_NAME, DataType.JSON)
        .build();
    expectedJsonNode = CustomObjectNode.create().setAll(TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(TEST_JSON_DOT_FIELD_NAME, TEST_JSON_STRING_NODE)
        .set(INDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)));
    transformWithIndexableFields(schema, inputJsonNode, expectedJsonNode, true);
  }

  @Test
  public void testWithUnindexableFieldsAndMergedTextIndex() {
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "stringField":"a",
      "intField_noIndex":9,
      "string_noIndex":"z",
      "message": "a",
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
            .set(TEST_JSON_NULL_FIELD_NAME, TEST_JSON_NULL_NODE)
            .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE_WITH_UPEERCASE)
            .set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
            .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
            .set(TEST_JSON_MESSAGE_NAME, TEST_JSON_STRING_NODE)
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
        "stringField":"aA_123",
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
        see the value of expectedJsonNodeWithMergedTextIndex
      ]
    }
    */
    expectedJsonNode = CustomObjectNode.create().set(INDEXABLE_EXTRAS_FIELD_NAME,
        CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, TEST_JSON_ARRAY_NODE)
            .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE_WITH_UPEERCASE)
            .set(TEST_JSON_MAP_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD).set(TEST_JSON_NESTED_MAP_FIELD_NAME,
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

    expectedJsonNodeWithMergedTextIndex = expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME, N.arrayNode()
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "arrayField"
            + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "aA_123" + JSON_KEY_VALUE_SEPARATOR + "stringField"
            + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "mapField.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField"
                + ".arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "message" + MERGED_TEXT_INDEX_EOD_ANCHOR));
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
          "stringField":"aA_123"
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
            CustomObjectNode.create().set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE_WITH_UPEERCASE)
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

    expectedJsonNodeWithMergedTextIndex = expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME, N.arrayNode()
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "arrayField"
            + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "aA_123" + JSON_KEY_VALUE_SEPARATOR + "stringField"
            + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "mapField.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "message" + MERGED_TEXT_INDEX_EOD_ANCHOR));
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
      "stringField":"aA_123",
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
        .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE_WITH_UPEERCASE)
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
    expectedJsonNodeWithMergedTextIndex = expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME, N.arrayNode()
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "arrayField"
            + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "arrayField" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "aA_123" + JSON_KEY_VALUE_SEPARATOR + "stringField"
            + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "mapField.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "[0,1,2,3]" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "0" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "1" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "2" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "3" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.arrayField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "nestedFields.mapField.stringField"
                + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "a" + JSON_KEY_VALUE_SEPARATOR + "message" + MERGED_TEXT_INDEX_EOD_ANCHOR));
    transformWithUnIndexableFieldsAndMergedTextIndex(
        schemaBuilder.addMultiValueDimension(MERGED_TEXT_INDEX_FIELD_NAME, DataType.STRING).build(), inputJsonNode,
        expectedJsonNodeWithMergedTextIndex);
  }

  @Test
  public void testKeyValueTransformation() {
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "message_logtype": "a",
      "stringField":"a",
      "intField_noIndex":9,
      "string_noIndex":"z",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "stringField":"a",
        "stringField":"aA_123",
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
        "stringField":"aA_123",
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
            .set(TEST_JSON_MESSAGE_NAME, TEST_JSON_STRING_NODE)
            .set(TEST_JSON_MESSAGE_LOGTYPE_NAME, TEST_JSON_STRING_NODE)
            .set(TEST_JSON_NULL_FIELD_NAME, TEST_JSON_NULL_NODE)
            .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE_WITH_UPEERCASE)
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

    String destStrColumnName = "mystringname_all_lowercases";
    String destMapColumnName = "myMapName";
    // make array field as single value STRING, test the conversion function
    // drop the column nestedFields.mapFields
    // preserve the entire mapField value
    // preserve the nestedFields.arrayField value and test the conversion function
    // map the column someMeaningfulName to nestedFields.stringField
    // abandon the json_data extra field
    // mergedTextIndex should contain columns who are not in preserved or dropped list
    // mergedTextIndex should contain message_logtye
    schemaBuilder = createDefaultSchemaBuilder().addSingleValueDimension(TEST_JSON_ARRAY_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_STRING_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_MESSAGE_LOGTYPE_NAME, DataType.STRING)
        .addSingleValueDimension(destMapColumnName, DataType.STRING)
        .addSingleValueDimension(TEST_JSON_MAP_EXTRA_FIELD_NAME, DataType.JSON)
        .addSingleValueDimension(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, DataType.STRING)
        .addSingleValueDimension(destStrColumnName, DataType.STRING);

    Map<String, String> keyMapping = new HashMap<>() {
      {
        put(destStrColumnName, TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_STRING_FIELD_NAME);
        put(destMapColumnName, TEST_JSON_MAP_FIELD_NAME);
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
        add(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME);
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
      "message_logtype": "a",
      "nestedFields.arrayField":[0,1,2,3],
      "stringFiled":"aA_123"
      "mystringname_all_lowercases":"a",
      "myMapName":{
        "arrayField":[0,1,2,3],
        "stringField":"a",
        "stringField":"aA_123",
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
      "nestedField.arrayField":[0,1,2,3],
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
        // check mergedTextIndexNode
      ],
      __mergedTextIndex_delimeter: [
        // check mergedTextIndexNode
      ]
    }
    */
    expectedJsonNode = CustomObjectNode.create().set(TEST_JSON_ARRAY_FIELD_NAME, N.textNode("[0,1,2,3]"))
        .set(TEST_JSON_MESSAGE_LOGTYPE_NAME, TEST_JSON_STRING_NODE)
        .set(TEST_JSON_STRING_FIELD_NAME, TEST_JSON_STRING_NODE_WITH_UPEERCASE)
        .set(destStrColumnName, TEST_JSON_STRING_NODE)
        // For single value field, it would serialize the value whose format is slightly different
        .set(destMapColumnName, N.textNode("{\"arrayField\":[0,1,2,3],\"stringField\":\"a\",\"intField_noIndex\":9,"
            + "\"stringField_noIndex\":\"z\"}")).set(TEST_JSON_MAP_EXTRA_FIELD_NAME, TEST_JSON_MAP_NODE_WITH_NO_IDX)
        .set(TEST_JSON_NESTED_MAP_FIELD_NAME + "." + TEST_JSON_ARRAY_FIELD_NAME, N.textNode("[0,1,2,3]"))

        .set(UNINDEXABLE_EXTRAS_FIELD_NAME,
            CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)
                .set(TEST_JSON_MAP_NO_IDX_FIELD_NAME, TEST_JSON_MAP_NODE_WITHOUT_NULL_FIELD)
                .set(TEST_JSON_NESTED_MAP_FIELD_NAME,
                    CustomObjectNode.create().set(TEST_JSON_INT_NO_IDX_FIELD_NAME, TEST_INT_NODE)
                        .set(TEST_JSON_STRING_NO_IDX_FIELD_NAME, TEST_JSON_STRING_NO_IDX_NODE)));

    JsonNode mergedTextIndexNode = N.arrayNode().add(
        MERGED_TEXT_INDEX_BOD_ANCHOR + "arrayField" + JSON_KEY_VALUE_SEPARATOR + "0" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "arrayField" + JSON_KEY_VALUE_SEPARATOR + "1" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "arrayField" + JSON_KEY_VALUE_SEPARATOR + "2" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "arrayField" + JSON_KEY_VALUE_SEPARATOR + "3" + MERGED_TEXT_INDEX_EOD_ANCHOR)
        .add(MERGED_TEXT_INDEX_BOD_ANCHOR + "arrayField" + JSON_KEY_VALUE_SEPARATOR + "[0,1,2,3]"
            + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + destStrColumnName + JSON_KEY_VALUE_SEPARATOR + "a"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + TEST_JSON_STRING_FIELD_NAME + JSON_KEY_VALUE_SEPARATOR
                + TEST_JSON_STRING_NODE_WITH_UPEERCASE.textValue() + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + TEST_JSON_STRING_FIELD_NAME + JSON_KEY_VALUE_SEPARATOR
                + TEST_JSON_STRING_NODE_WITH_UPEERCASE.textValue().toLowerCase(Locale.ENGLISH)
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "mapFieldExtra.arrayField" + JSON_KEY_VALUE_SEPARATOR + "[0,1,2,3]"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "mapFieldExtra.stringField" + JSON_KEY_VALUE_SEPARATOR + "a"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "mapFieldExtra.arrayField" + JSON_KEY_VALUE_SEPARATOR + "0"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "mapFieldExtra.arrayField" + JSON_KEY_VALUE_SEPARATOR + "1"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "mapFieldExtra.arrayField" + JSON_KEY_VALUE_SEPARATOR + "2"
                + MERGED_TEXT_INDEX_EOD_ANCHOR).add(
            MERGED_TEXT_INDEX_BOD_ANCHOR + "mapFieldExtra.arrayField" + JSON_KEY_VALUE_SEPARATOR + "3"
                + MERGED_TEXT_INDEX_EOD_ANCHOR);
    expectedJsonNodeWithMergedTextIndex =
        expectedJsonNode.deepCopy().set(MERGED_TEXT_INDEX_FIELD_NAME, mergedTextIndexNode);
    transformKeyValueTransformation(null, UNINDEXABLE_EXTRAS_FIELD_NAME,
        MERGED_TEXT_INDEX_FIELD_NAME,
        schemaBuilder.addMultiValueDimension(MERGED_TEXT_INDEX_FIELD_NAME, DataType.STRING).build(), keyMapping,
        pathToDrop, pathToPreserve, pathToPreserveWithIndex, inputJsonNode, expectedJsonNodeWithMergedTextIndex);
  }

  private void transformWithIndexableFields(Schema schema, JsonNode inputRecordJsonNode, JsonNode ouputRecordJsonNode,
      boolean useAnonymousDotInFieldNames) {
    testTransform(INDEXABLE_EXTRAS_FIELD_NAME, null, null, useAnonymousDotInFieldNames, false, false, schema, null,
        null, null, null,
        inputRecordJsonNode.toString(), ouputRecordJsonNode.toString());
  }

  private void transformWithUnIndexableFieldsAndMergedTextIndex(Schema schema, JsonNode inputRecordJsonNode,
      JsonNode ouputRecordJsonNode) {
    testTransform(INDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_EXTRAS_FIELD_NAME, null, true, false, null, schema, null,
        null,
        null, null, inputRecordJsonNode.toString(), ouputRecordJsonNode.toString());
  }

  private void transformKeyValueTransformation(String indexableExtraField, String unindeableExtraField,
      String mergedTextIndexField, Schema schema, Map<String, String> keyMapping, Set<String> fieldPathsToDrop,
      Set<String> fieldPathsToPreserve, Set<String> fieldPathsToPreserveWithIndex, JsonNode inputRecordJsonNode,
      JsonNode ouputRecordJsonNode) {
    testTransform(indexableExtraField, unindeableExtraField, mergedTextIndexField, true, true, false, schema,
        keyMapping,
        fieldPathsToDrop, fieldPathsToPreserve, fieldPathsToPreserveWithIndex, inputRecordJsonNode.toString(),
        ouputRecordJsonNode.toString());
  }

  private void testTransform(String indexableExtrasField, String unindexableExtrasField,
      String mergedTextIndexField, boolean useAnonymousDotInFieldNames, boolean optimizeCaseInsensitiveSearch,
      Boolean reverseTextIndexKeyValueOrder,
      Schema schema, Map<String, String> keyMapping, Set<String> fieldPathsToDrop, Set<String> fieldPathsToPreserve,
      Set<String> fieldPathsToPreserveWithIndex, String inputRecordJSONString, String expectedOutputRecordJSONString) {
    TableConfig tableConfig =
        createDefaultTableConfig(indexableExtrasField, unindexableExtrasField, UNINDEXABLE_FIELD_SUFFIX,
            fieldPathsToDrop, fieldPathsToPreserve, fieldPathsToPreserveWithIndex, keyMapping, mergedTextIndexField,
            useAnonymousDotInFieldNames,
            optimizeCaseInsensitiveSearch, reverseTextIndexKeyValueOrder);
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
    SchemaConformingTransformer schemaConformingTransformer =
        new SchemaConformingTransformer(tableConfig, schema);
    return schemaConformingTransformer.transform(inputRecord);
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
      SchemaConformingTransformer.validateSchema(schema,
          new SchemaConformingTransformerConfig(null, INDEXABLE_EXTRAS_FIELD_NAME, null, null, null, null, null, null,
              null, null, null, null, null, null, null, null, null, null, null, null, null, null));
    } catch (Exception ex) {
      fail("Should not have thrown any exception when overlapping schema occurs");
    }

    try {
      // This is a repeat of the previous test but with fields reversed just in case they are processed in order
      Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("a.b.c", DataType.INT)
          .addSingleValueDimension("a.b", DataType.STRING).build();
      SchemaConformingTransformer.validateSchema(schema,
          new SchemaConformingTransformerConfig(null, INDEXABLE_EXTRAS_FIELD_NAME, null, null, null, null, null, null,
              null, null, null, null, null, null, null, null, null, null, null, null, null, null));
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
    String longBinaryDataWithColon = "field:1:1:v1Cgy+ypzk8yf9JzsdkBjvZ1jM8Mem/BTtNilst64Df/34xmJzeRstmihpfrWZ";
    String jsonBinaryData = "{\"field\":\"text:1:1:v1Cgy+ypzk8yf9JzsdkBjvZ1jM8Mem/BTtNilst64Df/34xmJzeRstmihpfrWZ\"}";
    int minLength = 10;

    // A space is not expected in a based64 encoded string.
    assertFalse(SchemaConformingTransformer.base64ValueFilter(text.getBytes(), minLength));
    assertTrue(SchemaConformingTransformer.base64ValueFilter(binaryData.getBytes(), minLength));
    assertTrue(SchemaConformingTransformer.base64ValueFilter(binaryDataWithTrailingPeriods.getBytes(), minLength));
    assertFalse(SchemaConformingTransformer.base64ValueFilter(binaryDataWithRandomPeriods.getBytes(), minLength));
    assertFalse(SchemaConformingTransformer.base64ValueFilter(shortBinaryData.getBytes(), minLength));
    // A colon : is not expected in base64 encoded string.
    assertFalse(SchemaConformingTransformer.base64ValueFilter(longBinaryDataWithColon.getBytes(), minLength));
    // Json string can not be detected as base64 encoded string even one field has base64 encoded strings.
    assertFalse(SchemaConformingTransformer.base64ValueFilter(jsonBinaryData.getBytes(), minLength));
  }

  @Test
  public void testCreateSchemaConformingTransformerConfig() throws Exception {
    String ingestionConfigJson = "{"
        + "\"schemaConformingTransformerConfig\": {"
        + "  \"enableIndexableExtras\": false"
        + "}"
        + "}";

    IngestionConfig ingestionConfig = JsonUtils.stringToObject(ingestionConfigJson, IngestionConfig.class);
    SchemaConformingTransformerConfig config = ingestionConfig.getSchemaConformingTransformerConfig();
    assertNotNull(config);
    assertEquals(config.isEnableIndexableExtras(), false);

    // Backward compatibility test, V2 config should be able to create schemaConformingTransformerConfig
    ingestionConfigJson = "{"
        + "\"schemaConformingTransformerV2Config\": {"
        + "  \"enableIndexableExtras\": false"
        + "}"
        + "}";

    ingestionConfig = JsonUtils.stringToObject(ingestionConfigJson, IngestionConfig.class);
    config = ingestionConfig.getSchemaConformingTransformerConfig();
    assertNotNull(config);
    assertEquals(config.isEnableIndexableExtras(), false);
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
