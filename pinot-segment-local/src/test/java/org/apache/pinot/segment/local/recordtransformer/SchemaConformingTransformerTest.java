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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.fail;


public class SchemaConformingTransformerTest {
  static final private String INDEXABLE_EXTRAS_FIELD_NAME = "indexableExtras";
  static final private String UNINDEXABLE_EXTRAS_FIELD_NAME = "unindexableExtras";
  static final private String UNINDEXABLE_FIELD_SUFFIX = "_noIndex";

  static final private ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TableConfig createDefaultTableConfig(String indexableExtrasField, String unindexableExtrasField,
      String unindexableFieldSuffix, Set<String> fieldPathsToDrop) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    SchemaConformingTransformerConfig schemaConformingTransformerConfig =
        new SchemaConformingTransformerConfig(indexableExtrasField, unindexableExtrasField, unindexableFieldSuffix,
            fieldPathsToDrop);
    ingestionConfig.setSchemaConformingTransformerConfig(schemaConformingTransformerConfig);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig)
        .build();
  }

  private Schema.SchemaBuilder createDefaultSchemaBuilder() {
    return new Schema.SchemaBuilder().addSingleValueDimension(INDEXABLE_EXTRAS_FIELD_NAME, DataType.JSON)
        .addSingleValueDimension(UNINDEXABLE_EXTRAS_FIELD_NAME, DataType.JSON);
  }

  @Test
  public void testWithNoUnindexableFields() {
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nullField":null,
      "stringField":"a",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "nestedFields":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a"
        }
      }
    }
    */
    final String inputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"},\"nestedFields\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,"
            + "\"stringField\":\"a\"}}}";
    String expectedOutputRecordJSONString;
    Schema schema;

    schema = createDefaultSchemaBuilder().build();
    /*
    {
      "indexableExtras":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a"
        },
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a",
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "nullField":null,
            "stringField":"a"
          }
        }
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"indexableExtras\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"}}}}";
    testTransformWithNoUnindexableFields(schema, inputRecordJSONString, expectedOutputRecordJSONString);

    schema = createDefaultSchemaBuilder().addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING).build();
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "nestedFields.stringField":"a",
      "indexableExtras":{
        "nullField":null,
        "stringField":"a",
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "nullField":null,
            "stringField":"a"
          }
        }
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields.stringField\":\"a\",\"indexableExtras\":{\"nullField\":null,\"stringField\":\"a\","
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"}}}}";
    testTransformWithNoUnindexableFields(schema, inputRecordJSONString, expectedOutputRecordJSONString);

    schema = createDefaultSchemaBuilder().addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("nullField", DataType.STRING).addSingleValueDimension("stringField", DataType.STRING)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addMultiValueDimension("nestedFields.arrayField", DataType.INT)
        .addSingleValueDimension("nestedFields.nullField", DataType.STRING)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING)
        .addSingleValueDimension("nestedFields.mapField", DataType.JSON).build();
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nullField":null,
      "stringField":"a",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "nestedFields.arrayField":[0, 1, 2, 3],
      "nestedFields.nullField":null,
      "nestedFields.stringField":"a",
      "nestedFields.mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"},\"nestedFields.arrayField\":[0,1,2,3],\"nestedFields"
            + ".nullField\":null,\"nestedFields.stringField\":\"a\",\"nestedFields.mapField\":{\"arrayField\":[0,1,2,"
            + "3],\"nullField\":null,\"stringField\":\"a\"}}";
    testTransformWithNoUnindexableFields(schema, inputRecordJSONString, expectedOutputRecordJSONString);
  }

  private void testTransformWithNoUnindexableFields(Schema schema, String inputRecordJSONString,
      String expectedOutputRecordJSONString) {
    testTransform(null, null, schema, null, inputRecordJSONString, expectedOutputRecordJSONString);
    testTransform(null, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString, expectedOutputRecordJSONString);
    testTransform(UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString,
        expectedOutputRecordJSONString);
  }

  @Test
  public void testWithUnindexableFields() {
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nullField":null,
      "stringField":"a",
      "intField_noIndex":9,
      "string_noIndex":"z",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z"
      },
      "nestedFields":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a",
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a",
          "intField_noIndex":9,
          "string_noIndex":"z"
        }
      }
    }
    */
    final String inputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"intField_noIndex\":9,"
            + "\"string_noIndex\":\"z\",\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,"
            + "\"stringField\":\"a\",\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"intField_noIndex\":9,\"string_noIndex\":\"z\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\",\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}";
    String expectedOutputRecordJSONString;
    Schema schema;

    schema = createDefaultSchemaBuilder().build();
    /*
    {
      "indexableExtras":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a"
        },
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a",
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "nullField":null,
            "stringField":"a"
          }
        }
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"indexableExtras\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"}}}}";
    testTransform(null, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString, expectedOutputRecordJSONString);
    /*
    {
      "indexableExtras":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a",
        "mapField":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a"
        },
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "stringField":"a",
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "nullField":null,
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
        "nestedFields":{
          "intField_noIndex":9,
          "string_noIndex":"z",
          "mapField":{
            "intField_noIndex":9,
            "string_noIndex":"z"
          }
        }
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"indexableExtras\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"}}},"
            + "\"unindexableExtras\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}}";
    testTransform(UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString,
        expectedOutputRecordJSONString);

    schema = createDefaultSchemaBuilder().addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING).build();
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "nestedFields.stringField":"a",
      "indexableExtras":{
        "nullField":null,
        "stringField":"a",
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "nullField":null,
            "stringField":"a"
          }
        }
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields.stringField\":\"a\",\"indexableExtras\":{\"nullField\":null,\"stringField\":\"a\","
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"}}}}";
    testTransform(null, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString, expectedOutputRecordJSONString);
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "nestedFields.stringField":"a",
      "indexableExtras":{
        "nullField":null,
        "stringField":"a",
        "nestedFields":{
          "arrayField":[0, 1, 2, 3],
          "nullField":null,
          "mapField":{
            "arrayField":[0, 1, 2, 3],
            "nullField":null,
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
        "nestedFields":{
          "intField_noIndex":9,
          "string_noIndex":"z",
          "mapField":{
            "intField_noIndex":9,
            "string_noIndex":"z"
          }
        }
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields.stringField\":\"a\",\"indexableExtras\":{\"nullField\":null,\"stringField\":\"a\","
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"}}},\"unindexableExtras\":{\"intField_noIndex\":9,"
            + "\"string_noIndex\":\"z\",\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}}";
    testTransform(UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString,
        expectedOutputRecordJSONString);

    schema = createDefaultSchemaBuilder().addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("nullField", DataType.STRING).addSingleValueDimension("stringField", DataType.STRING)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addMultiValueDimension("nestedFields.arrayField", DataType.INT)
        .addSingleValueDimension("nestedFields.nullField", DataType.STRING)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING)
        .addSingleValueDimension("nestedFields.mapField", DataType.JSON).build();
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nullField":null,
      "stringField":"a",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "nestedFields.arrayField":[0, 1, 2, 3],
      "nestedFields.nullField":null,
      "nestedFields.stringField":"a",
      "nestedFields.mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"},\"nestedFields.arrayField\":[0,1,2,3],\"nestedFields"
            + ".nullField\":null,\"nestedFields.stringField\":\"a\",\"nestedFields.mapField\":{\"arrayField\":[0,1,2,"
            + "3],\"nullField\":null,\"stringField\":\"a\"} }";
    testTransform(null, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString, expectedOutputRecordJSONString);
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nullField":null,
      "stringField":"a",
      "mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "nestedFields.arrayField":[0, 1, 2, 3],
      "nestedFields.nullField":null,
      "nestedFields.stringField":"a",
      "nestedFields.mapField":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a"
      },
      "unindexableExtras":{
        "intField_noIndex":9,
        "string_noIndex":"z",
        "mapField":{
          "intField_noIndex":9,
          "string_noIndex":"z"
        },
        "nestedFields":{
          "intField_noIndex":9,
          "string_noIndex":"z",
          "mapField":{
            "intField_noIndex":9,
            "string_noIndex":"z"
          }
        }
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"},\"nestedFields.arrayField\":[0,1,2,3],\"nestedFields"
            + ".nullField\":null,\"nestedFields.stringField\":\"a\",\"nestedFields.mapField\":{\"arrayField\":[0,1,2,"
            + "3],\"nullField\":null,\"stringField\":\"a\"},\"unindexableExtras\":{\"intField_noIndex\":9,"
            + "\"string_noIndex\":\"z\",\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}}";
    testTransform(UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX, schema, null, inputRecordJSONString,
        expectedOutputRecordJSONString);
  }

  @Test
  public void testFieldPathsToDrop() {
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nullField":null,
      "stringField":"a",
      "boolField":false,
      "nestedFields":{
        "arrayField":[0, 1, 2, 3],
        "nullField":null,
        "stringField":"a",
        "boolField":false
      }
    }
    */
    final String inputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"boolField\":false,"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"boolField\":false}}";
    String expectedOutputRecordJSONString;
    Schema schema;

    schema = createDefaultSchemaBuilder().addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("nullField", DataType.STRING)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING)
        .addSingleValueDimension("nestedFields.boolField", DataType.BOOLEAN).build();
    Set<String> fieldPathsToDrop = new HashSet<>(Arrays.asList("stringField", "nestedFields.arrayField"));
    /*
    {
      "arrayField":[0, 1, 2, 3],
      "nullField":null,
      "indexableExtras": {
        "boolField":false,
        "nestedFields": {
          nullField":null
        }
      },
      "nestedFields":{
        "stringField":"a",
        "boolField":false
      }
    }
    */
    expectedOutputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"nestedFields.stringField\":\"a\",\"nestedFields"
            + ".boolField\":false,\"indexableExtras\":{\"boolField\":false,\"nestedFields\":{\"nullField\":null}}}";
    testTransform(UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX, schema, fieldPathsToDrop,
        inputRecordJSONString, expectedOutputRecordJSONString);
  }

  @Test
  public void testIgnoringSpecialRowKeys() {
    // Configure a FilterTransformer and a SchemaConformingTransformer such that the filter will introduce a special
    // key $(SKIP_RECORD_KEY$) that the SchemaConformingTransformer should ignore
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setFilterConfig(new FilterConfig("intField = 1"));
    SchemaConformingTransformerConfig schemaConformingTransformerConfig =
        new SchemaConformingTransformerConfig(INDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_EXTRAS_FIELD_NAME,
            UNINDEXABLE_FIELD_SUFFIX, null);
    ingestionConfig.setSchemaConformingTransformerConfig(schemaConformingTransformerConfig);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();

    // Create a series of transformers: FilterTransformer -> SchemaConformingTransformer
    List<RecordTransformer> transformers = new LinkedList<>();
    transformers.add(new FilterTransformer(tableConfig));
    Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("intField", DataType.INT).build();
    transformers.add(new SchemaConformingTransformer(tableConfig, schema));
    CompositeTransformer compositeTransformer = new CompositeTransformer(transformers);

    Map<String, Object> inputRecordMap = jsonStringToMap("{\"intField\":1}");
    GenericRow inputRecord = createRowFromMap(inputRecordMap);
    GenericRow outputRecord = compositeTransformer.transform(inputRecord);
    Assert.assertNotNull(outputRecord);
    // Check that the transformed record has $SKIP_RECORD_KEY$
    Assert.assertFalse(IngestionUtils.shouldIngestRow(outputRecord));
  }

  @Test
  public void testOverlappingSchemaFields() {
    Assert.assertThrows(IllegalArgumentException.class, () -> {
      Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("a.b", DataType.STRING)
          .addSingleValueDimension("a.b.c", DataType.INT).build();
      SchemaConformingTransformer.validateSchema(schema,
          new SchemaConformingTransformerConfig(INDEXABLE_EXTRAS_FIELD_NAME, null, null, null));
    });

    // This is a repeat of the previous test but with fields reversed just in case they are processed in order
    Assert.assertThrows(IllegalArgumentException.class, () -> {
      Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("a.b.c", DataType.INT)
          .addSingleValueDimension("a.b", DataType.STRING).build();
      SchemaConformingTransformer.validateSchema(schema,
          new SchemaConformingTransformerConfig(INDEXABLE_EXTRAS_FIELD_NAME, null, null, null));
    });
  }

  @Test
  public void testInvalidFieldNamesInSchema() {
    // Ensure schema fields which end with unindexableFieldSuffix are caught as invalid
    Assert.assertThrows(() -> {
      Schema schema =
          createDefaultSchemaBuilder().addSingleValueDimension("a" + UNINDEXABLE_FIELD_SUFFIX, DataType.STRING)
              .addSingleValueDimension("a.b" + UNINDEXABLE_FIELD_SUFFIX, DataType.INT).build();
      SchemaConformingTransformer.validateSchema(schema,
          new SchemaConformingTransformerConfig(INDEXABLE_EXTRAS_FIELD_NAME, null, UNINDEXABLE_FIELD_SUFFIX, null));
    });

    // Ensure schema fields which are in fieldPathsToDrop are caught as invalid
    Assert.assertThrows(() -> {
      Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("a", DataType.STRING)
          .addSingleValueDimension("b.c", DataType.INT).build();
      Set<String> fieldPathsToDrop = new HashSet<>(Arrays.asList("a", "b.c"));
      SchemaConformingTransformer.validateSchema(schema,
          new SchemaConformingTransformerConfig(INDEXABLE_EXTRAS_FIELD_NAME, null, null, fieldPathsToDrop));
    });
  }

  @Test
  public void testSchemaRecordMismatch() {
    Schema schema =
        createDefaultSchemaBuilder().addSingleValueDimension("nestedFields.mapField", DataType.JSON).build();
    /*
    {
      "indexableExtras":{
        "nestedFields":0,
      }
    }
    */
    // Schema field "nestedFields.map" is a Map but the record field is an int, so it should be stored in
    // indexableExtras
    testTransform(UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX, schema, null, "{\"nestedFields\":0}",
        "{\"indexableExtras\":{\"nestedFields\":0}}");
  }

  @Test
  public void testFieldTypesForExtras() {
    final String inputRecordJSONString = "{\"arrayField\":[0,1,2,3]}";

    TableConfig tableConfig =
        createDefaultTableConfig(INDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX,
            null);
    Schema validSchema =
        new Schema.SchemaBuilder().addSingleValueDimension(INDEXABLE_EXTRAS_FIELD_NAME, DataType.STRING)
            .addSingleValueDimension(UNINDEXABLE_EXTRAS_FIELD_NAME, DataType.STRING).build();
    GenericRow outputRecord = transformRow(tableConfig, validSchema, inputRecordJSONString);

    Assert.assertNotNull(outputRecord);
    // Validate that the indexable extras field contains the input record as a string
    Assert.assertEquals(outputRecord.getValue(INDEXABLE_EXTRAS_FIELD_NAME), inputRecordJSONString);

    // Validate that invalid field types are caught
    Schema invalidSchema = new Schema.SchemaBuilder().addSingleValueDimension(INDEXABLE_EXTRAS_FIELD_NAME, DataType.INT)
        .addSingleValueDimension(UNINDEXABLE_EXTRAS_FIELD_NAME, DataType.BOOLEAN).build();
    Assert.assertThrows(() -> {
      transformRow(tableConfig, invalidSchema, inputRecordJSONString);
    });
  }

  @Test
  public void testInvalidTransformerConfig() {
    Assert.assertThrows(() -> {
      createDefaultTableConfig(null, null, null, null);
    });
    Assert.assertThrows(() -> {
      createDefaultTableConfig(null, UNINDEXABLE_EXTRAS_FIELD_NAME, null, null);
    });
    Assert.assertThrows(() -> {
      createDefaultTableConfig(null, null, UNINDEXABLE_FIELD_SUFFIX, null);
    });
    Assert.assertThrows(() -> {
      createDefaultTableConfig(null, UNINDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_FIELD_SUFFIX, null);
    });
    Assert.assertThrows(() -> {
      createDefaultTableConfig(INDEXABLE_EXTRAS_FIELD_NAME, UNINDEXABLE_EXTRAS_FIELD_NAME, null, null);
    });
  }

  /**
   * Validates transforming the given row results in the expected row, where both rows are given as JSON strings
   */
  private void testTransform(String unindexableExtrasField, String unindexableFieldSuffix, Schema schema,
      Set<String> fieldPathsToDrop, String inputRecordJSONString, String expectedOutputRecordJSONString) {
    TableConfig tableConfig =
        createDefaultTableConfig(INDEXABLE_EXTRAS_FIELD_NAME, unindexableExtrasField, unindexableFieldSuffix,
            fieldPathsToDrop);
    GenericRow outputRecord = transformRow(tableConfig, schema, inputRecordJSONString);

    Assert.assertNotNull(outputRecord);
    Map<String, Object> expectedOutputRecordMap = jsonStringToMap(expectedOutputRecordJSONString);
    Assert.assertEquals(outputRecord.getFieldToValueMap(), expectedOutputRecordMap);
  }

  /**
   * Transforms the given row (given as a JSON string) using the transformer
   * @return The transformed row
   */
  private GenericRow transformRow(TableConfig tableConfig, Schema schema, String inputRecordJSONString) {
    Map<String, Object> inputRecordMap = jsonStringToMap(inputRecordJSONString);
    GenericRow inputRecord = createRowFromMap(inputRecordMap);
    SchemaConformingTransformer schemaConformingTransformer = new SchemaConformingTransformer(tableConfig, schema);
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
}
