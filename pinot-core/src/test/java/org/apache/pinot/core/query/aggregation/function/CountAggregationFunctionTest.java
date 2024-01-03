/*
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
 *
 */

package org.apache.pinot.core.query.aggregation.function;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CountAggregationFunctionTest {

  private File _baseDir;

  private final Schema _nullableIntSchema = new Schema.SchemaBuilder()
      .setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addDimensionField("myInt", FieldSpec.DataType.INT, f -> f.setNullable(true))
      .build();

  private final TableConfig _nullableIntTableConfig = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("testTable")
      .build();

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
  public void list() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(_nullableIntSchema, _nullableIntTableConfig)
        .onFirstInstance(
            new Object[] {1}
        )
        .andOnSecondInstance(
            new Object[] {2},
            new Object[] {null}
        )
        .whenQuery("select myInt from testTable order by myInt")
        .thenResultIs("INTEGER",
            "-2147483648",
            "1",
            "2"
        );
  }

  @Test
  public void listNullHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(_nullableIntSchema, _nullableIntTableConfig)
        .onFirstInstance(
            new Object[] {1}
        )
        .andOnSecondInstance(
            new Object[] {2},
            new Object[] {null}
        )
        .whenQuery("select myInt from testTable order by myInt")
        .thenResultIs("INTEGER",
            "1",
            "2",
            "null"
        );
  }

  @Test
  public void countNullWhenHandlingDisabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(_nullableIntSchema, _nullableIntTableConfig)
        .onFirstInstance(
            "myInt",
            "1"
        )
        .andOnSecondInstance(
            "myInt",
            "2",
            "null"
        )
        .whenQuery("select myInt, COUNT(myInt) from testTable group by myInt order by myInt")
        .thenResultIs("INTEGER | LONG",
            "-2147483648 | 1",
            "1           | 1",
            "2           | 1"
        );
  }


  @Test
  public void countNullWhenHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(_nullableIntSchema, _nullableIntTableConfig)
        .onFirstInstance(
            "myInt",
            "1"
        )
        .andOnSecondInstance(
            "myInt",
            "2",
            "null"
        )
        .whenQuery("select myInt, COUNT(myInt) from testTable group by myInt order by myInt")
        .thenResultIs(
            "INTEGER | LONG",
            "1    | 1",
            "2    | 1",
            "null | 0"
        );
  }

  @Test
  public void countStarNullWhenHandlingDisabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(_nullableIntSchema, _nullableIntTableConfig)
        .onFirstInstance(
            "myInt",
            "1"
        )
        .andOnSecondInstance(
            "myInt",
            "2",
            "null"
        )
        .whenQuery("select myInt, COUNT(*) from testTable group by myInt order by myInt")
        .thenResultIs("INTEGER | LONG",
            "-2147483648 | 1",
            "1    | 1",
            "2    | 1"
        );
  }

  @Test
  public void countStarNullWhenHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(_nullableIntSchema, _nullableIntTableConfig)
        .onFirstInstance(
            "myInt",
            "1"
        )
        .andOnSecondInstance(
            "myInt",
            "2",
            "null"
        )
        .whenQuery("select myInt, COUNT(*) from testTable group by myInt order by myInt")
        .thenResultIs("INTEGER | LONG",
            "1    | 1",
            "2    | 1",
            "null | 1"
        );;
  }
}
