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
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class NullQueriesFluentTest {

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("testTable")
      .addFieldConfig(
          new FieldConfig.Builder("stringCol")
              .build())
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
      .build();

  // Selection ORDER BY on an identifier with a LIMIT plans MinMaxValueBasedSelectionOrderByCombineOperator, which skips
  // segments whose min/max cannot beat the current boundary. The tests below put two segments on one instance so the
  // skip can happen. With a single instance, BaseQueriesTest serves it as two identical servers, so the broker merges
  // two copies of the combine result before applying the LIMIT. The comments on each assertion give the combine
  // (per-server) result the expectation is derived from.
  private static final TableConfig NULL_HANDLING_TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("testTable")
      .setNullHandlingEnabled(true)
      .build();

  private static final TableConfig PLAIN_TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("testTable")
      .build();

  private static final Schema INT_SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName("testTable")
      .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
      .build();

  private static final Schema INT_CUSTOM_DEFAULT_NULL_SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName("testTable")
      .addSingleValueDimension("intCol", FieldSpec.DataType.INT, 1000)
      .build();

  private static final Schema INT_COLUMN_BASED_SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addDimensionField("intCol", FieldSpec.DataType.INT, fieldSpec -> fieldSpec.setNullable(true))
      .build();

  // One thread processes the segments in min/max order, which makes the segment skip deterministic
  private static final Map<String, String> SINGLE_THREAD = Map.of("maxExecutionThreads", "1");

  private File _baseDir;

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

  @Test
  public void testCastStringToTimestampNullHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(SCHEMA, TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"2025-09-23T17:38:00"},
            new Object[]{null}
        ).andOnSecondInstance(
            new Object[]{"2025-09-23T17:38:00"},
            new Object[]{null}
        )
        .whenQuery("select cast(stringCol as timestamp) from testTable")
        .thenResultIs(
            new Object[]{"2025-09-23 17:38:00.0"},
            new Object[]{null},
            new Object[]{"2025-09-23 17:38:00.0"},
            new Object[]{null}
        );
  }

  @Test
  public void testCastStringToTimestampNullHandlingDisabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SCHEMA, TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"2025-09-23T17:38:00"},
            new Object[]{null}
        ).andOnSecondInstance(
            new Object[]{"2025-09-23T17:38:00"},
            new Object[]{null}
        )
        // The IS NOT NULL predicate is required when null handling is disabled, since the default string null value
        // is "null", and that can't be cast to a valid timestamp.
        .whenQuery("select cast(stringCol as timestamp) from testTable where stringCol is not null")
        .thenResultIs(
            new Object[]{"2025-09-23 17:38:00.0"},
            new Object[]{"2025-09-23 17:38:00.0"}
        );
  }

  /// The segment with nulls has max 5, below the boundary 100 set by the first segment, but its nulls sort first.
  @Test
  public void testMinMaxCombineOrderByDescKeepsNullsFirst() {
    givenHighAndLowWithNullsSegments(INT_SCHEMA, NULL_HANDLING_TABLE_CONFIG)
        // Combine result: [null, null, 102]; the two copies hold 4 nulls, so LIMIT 3 keeps only nulls
        .whenQuery("select intCol from testTable order by intCol desc limit 3")
        .thenResultIs(new Object[]{null}, new Object[]{null}, new Object[]{null})
        // Combine result: [null, null, 102]
        .whenQuery("select intCol from testTable order by intCol desc nulls first limit 3")
        .thenResultIs(new Object[]{null}, new Object[]{null}, new Object[]{null});
  }

  @Test
  public void testMinMaxCombineOrderByDescKeepsNullsFirstColumnBasedNullHandling() {
    givenHighAndLowWithNullsSegments(INT_COLUMN_BASED_SCHEMA, PLAIN_TABLE_CONFIG)
        // Combine result: [null, null, 102]
        .whenQuery("select intCol from testTable order by intCol desc limit 3")
        .thenResultIs(new Object[]{null}, new Object[]{null}, new Object[]{null});
  }

  /// Orders where nulls sort last are not affected by the segment skip.
  @Test
  public void testMinMaxCombineNullsLast() {
    givenHighAndLowWithNullsSegments(INT_SCHEMA, NULL_HANDLING_TABLE_CONFIG)
        // Combine result: [102, 101, 100]
        .whenQuery("select intCol from testTable order by intCol desc nulls last limit 3")
        .thenResultIs(new Object[]{102}, new Object[]{102}, new Object[]{101})
        // Combine result: [5, 100, 101]
        .whenQuery("select intCol from testTable order by intCol limit 3")
        .thenResultIs(new Object[]{5}, new Object[]{5}, new Object[]{100})
        // Combine result: [5, 100, 101]
        .whenQuery("select intCol from testTable order by intCol asc nulls last limit 3")
        .thenResultIs(new Object[]{5}, new Object[]{5}, new Object[]{100});
  }

  /// The stored default null value (1000) gives the segment with nulls min 500, above the boundary 3 set by the first
  /// segment, but its nulls sort first.
  @Test
  public void testMinMaxCombineOrderByAscNullsFirstWithCustomDefaultNullValue() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withExtraQueryOptions(SINGLE_THREAD)
        .withNullHandling(true)
        .givenTable(INT_CUSTOM_DEFAULT_NULL_SCHEMA, NULL_HANDLING_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1},
            new Object[]{2},
            new Object[]{3}
        ).andSegment(
            new Object[]{500},
            new Object[]{null},
            new Object[]{null}
        )
        // Combine result: [null, null, 1]
        .whenQuery("select intCol from testTable order by intCol asc nulls first limit 3")
        .thenResultIs(new Object[]{null}, new Object[]{null}, new Object[]{null});
  }

  /// The segment with nulls (max 95) is not skipped by the boundary 80, and the last of its top 3 rows is null, which
  /// must not be used as a boundary value.
  @Test
  public void testMinMaxCombineNullBoundaryValue() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withExtraQueryOptions(SINGLE_THREAD)
        .withNullHandling(true)
        .givenTable(INT_SCHEMA, NULL_HANDLING_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{100},
            new Object[]{90},
            new Object[]{80}
        ).andSegment(
            new Object[]{95},
            new Object[]{null},
            new Object[]{null}
        )
        // Combine result: [100, 95, 90]
        .whenQuery("select intCol from testTable order by intCol desc nulls last limit 3")
        .thenResultIs(new Object[]{100}, new Object[]{100}, new Object[]{95})
        // Combine result: [null, null, 100]
        .whenQuery("select intCol from testTable order by intCol desc limit 3")
        .thenResultIs(new Object[]{null}, new Object[]{null}, new Object[]{null});
  }

  private FluentQueryTest.OnFirstInstance givenHighAndLowWithNullsSegments(Schema schema, TableConfig tableConfig) {
    return FluentQueryTest.withBaseDir(_baseDir)
        .withExtraQueryOptions(SINGLE_THREAD)
        .withNullHandling(true)
        .givenTable(schema, tableConfig)
        .onFirstInstance(
            new Object[]{100},
            new Object[]{101},
            new Object[]{102}
        ).andSegment(
            new Object[]{5},
            new Object[]{null},
            new Object[]{null}
        );
  }
}
