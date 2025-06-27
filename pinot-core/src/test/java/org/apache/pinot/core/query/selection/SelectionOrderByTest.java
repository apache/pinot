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

package org.apache.pinot.core.query.selection;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/** query-based tests for selection-orderby */
public class SelectionOrderByTest {

  @Test
  public void list() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1},
            new Object[]{3}
        )
        .andOnSecondInstance(
            new Object[]{2},
            new Object[]{null}
        )
        .whenQuery("select myField from testTable order by myField")
        .thenResultIs("INTEGER",
            "-2147483648",
            "1",
            "2",
            "3"
        );
  }

  @Test
  public void listNullHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1},
            new Object[]{3}
        )
        .andOnSecondInstance(
            new Object[]{2},
            new Object[]{null}
        )
        .whenQuery("select myField from testTable order by myField")
        .thenResultIs("INTEGER",
            "1",
            "2",
            "3",
            "null"
        );
  }

  @Test
  public void listTwoFields() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, null}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1")
        .thenResultIs("INTEGER|INTEGER",
            "-2147483648|-2147483648",
            "1|5",
            "2|3",
            "3|4"
        );
  }

  @Test
  public void listTwoFieldsNullHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1")
        .thenResultIs("INTEGER|INTEGER",
            "1|5",
            "2|3",
            "3|4",
            "null|2"
        );
  }

  @Test
  public void listTwoFieldsNullHandlingEnabledNullsFirst() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1 nulls first")
        .thenResultIs("INTEGER|INTEGER",
            "null|2",
            "1|5",
            "2|3",
            "3|4"
        );
  }

  @Test
  public void listTwoFieldsDesc() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, null}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1 desc")
        .thenResultIs("INTEGER|INTEGER",
            "3|4",
            "2|3",
            "1|5",
            "-2147483648|-2147483648"
        );
  }

  @Test
  public void listTwoFieldsNullHandlingEnabledDesc() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{4, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{3, 0},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1 desc")
        .thenResultIs("INTEGER|INTEGER",
            "null|2",
            "4|4",
            "3|0",
            "2|3",
            "1|5"
        );
  }

  @Test
  public void listTwoFieldsNullHandlingEnabledDescNullsLast() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{4, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{3, 0},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1 desc nulls last")
        .thenResultIs("INTEGER|INTEGER",
            "4|4",
            "3|0",
            "2|3",
            "1|5",
            "null|2"
        );
  }

  @Test
  public void listSortonTwoFields() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4},
            new Object[]{2, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1, field2")
        .thenResultIs("INTEGER|INTEGER",
            "-2147483648|2",
            "1|5",
            "2|3",
            "2|4",
            "3|4"
        );
  }

  @Test
  public void listSortonTwoFieldsNullHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4},
            new Object[]{2, 4},
            new Object[]{null, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1, field2")
        .thenResultIs("INTEGER|INTEGER",
            "1|5",
            "2|3",
            "2|4",
            "3|4",
            "null|2",
            "null|4"
        );
  }

  @Test
  public void listSortonTwoFieldsOneDesc() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4},
            new Object[]{2, 4},
            new Object[]{null, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1 desc, field2")
        .thenResultIs("INTEGER|INTEGER",
            "3|4",
            "2|3",
            "2|4",
            "1|5",
            "-2147483648|2",
            "-2147483648|4"
        );
  }

  @Test
  public void listSortonTwoFieldsNullHandlingEnabledOneDesc() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), TWO_FIELDS_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, 5},
            new Object[]{3, 4},
            new Object[]{2, 4},
            new Object[]{null, 4}
        )
        .andOnSecondInstance(
            new Object[]{2, 3},
            new Object[]{null, 2}
        )
        .whenQuery("select field1, field2 from testTable2 order by field1 desc, field2")
        .thenResultIs("INTEGER|INTEGER",
            "null|2",
            "null|4",
            "3|4",
            "2|3",
            "2|4",
            "1|5"
        );
  }

  @Test
  public void listOffset() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1},
            new Object[]{3}
        )
        .andOnSecondInstance(
            new Object[]{2},
            new Object[]{null}
        )
        .whenQuery("select myField from testTable order by myField offset 1")
        .thenResultIs("INTEGER",
            "1",
            "2",
            "3"
        );
  }

  @Test
  public void listOffsetLimit() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1},
            new Object[]{3}
        )
        .andOnSecondInstance(
            new Object[]{2},
            new Object[]{null}
        )
        .whenQuery("select myField from testTable order by myField offset 1 limit 2")
        .thenResultIs("INTEGER",
            "1",
            "2"
        );
  }

  @Test
  public void listOffsetLargerThanResult() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1},
            new Object[]{3}
        )
        .andOnSecondInstance(
            new Object[]{2},
            new Object[]{null}
        )
        .whenQuery("select myField from testTable order by myField offset 10")
        .thenResultIs("INTEGER"
        );
  }

  // utils ---

  @DataProvider(name = "nullHandlingEnabled")
  public Object[][] nullHandlingEnabled() {
    return new Object[][]{
        {false}, {true}
    };
  }

  private static final FieldSpec.DataType[] VALID_DATA_TYPES = new FieldSpec.DataType[]{
      FieldSpec.DataType.INT,
      FieldSpec.DataType.LONG,
      FieldSpec.DataType.FLOAT,
      FieldSpec.DataType.DOUBLE,
      FieldSpec.DataType.STRING,
      FieldSpec.DataType.BYTES,
      FieldSpec.DataType.BIG_DECIMAL,
      FieldSpec.DataType.TIMESTAMP,
      FieldSpec.DataType.BOOLEAN
  };

  protected static final Map<FieldSpec.DataType, Schema> SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS =
      Arrays.stream(VALID_DATA_TYPES)
          .collect(Collectors.toMap(dt -> dt, dt -> new Schema.SchemaBuilder()
              .setSchemaName("testTable")
              .setEnableColumnBasedNullHandling(true)
              .addDimensionField("myField", dt, f -> f.setNullable(true))
              .build()));

  protected static final Map<FieldSpec.DataType, Schema> TWO_FIELDS_NULLABLE_DIMENSION_SCHEMAS =
      Arrays.stream(VALID_DATA_TYPES)
          .collect(Collectors.toMap(dt -> dt, dt -> new Schema.SchemaBuilder()
              .setSchemaName("testTable2")
              .setEnableColumnBasedNullHandling(true)
              .addDimensionField("field1", dt, f -> f.setNullable(true))
              .addDimensionField("field2", dt, f -> f.setNullable(true))
              .build()));

  protected static final TableConfig SINGLE_FIELD_TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("testTable")
      .build();

  protected static final TableConfig TWO_FIELDS_TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("testTable")
      .build();

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
}
