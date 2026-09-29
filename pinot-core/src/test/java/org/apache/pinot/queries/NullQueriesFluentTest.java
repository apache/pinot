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
}
