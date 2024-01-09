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

package org.apache.pinot.core.query.aggregation.function;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class AbstractAggregationFunctionTest {

  protected File _baseDir;

  private static final FieldSpec.DataType[] VALID_DATA_TYPES = new FieldSpec.DataType[] {
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

  protected static final Map<FieldSpec.DataType, Schema> SINGLE_FIELD_NULLABLE_SCHEMAS = Arrays.stream(VALID_DATA_TYPES)
          .collect(Collectors.toMap(dt -> dt, dt -> new Schema.SchemaBuilder()
              .setSchemaName("testTable")
              .setEnableColumnBasedNullHandling(true)
              .addDimensionField("myField", dt, f -> f.setNullable(true))
              .build()));

  protected static final Map<FieldSpec.DataType, Schema> SINGLE_NULLABLE_FIELD_AND_MULTI_FIELD_SCHEMAS =
      Arrays.stream(VALID_DATA_TYPES)
          .collect(Collectors.toMap(dt -> dt, dt -> new Schema.SchemaBuilder()
              .setSchemaName("testTable")
              .setEnableColumnBasedNullHandling(true)
              .addDimensionField("myField", dt, f -> f.setNullable(true))
              .addMultiValueDimension("multiField", FieldSpec.DataType.STRING)
              .build()));

  protected static final TableConfig EMPTY_TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("testTable")
      .build();

  protected FluentQueryTest.DeclaringTable givenSingleNullableFieldTable(FieldSpec.DataType dataType,
      boolean nullHandlingEnabled) {
    return givenSingleNullableFieldTable(dataType, nullHandlingEnabled, null);
  }

  protected FluentQueryTest.DeclaringTable givenSingleNullableFieldTable(FieldSpec.DataType dataType,
      boolean nullHandlingEnabled, @Nullable Consumer<FieldConfig.Builder> customize) {
    TableConfig tableConfig;
    if (customize == null) {
      tableConfig = SINGLE_FIELD_TABLE_CONFIG;
    } else {
      TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable");
      FieldConfig.Builder fieldConfigBuilder = new FieldConfig.Builder("myField");
      customize.accept(fieldConfigBuilder);
      FieldConfig fieldConfig = fieldConfigBuilder.build();
      builder.setFieldConfigList(Collections.singletonList(fieldConfig));

      tableConfig = builder.build();
    }

    return FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(nullHandlingEnabled)
        .givenTable(SINGLE_FIELD_NULLABLE_SCHEMAS.get(dataType), tableConfig);
  }

  protected FluentQueryTest.DeclaringTable givenSingleNullableIntFieldTable(boolean nullHandling) {
    return givenSingleNullableFieldTable(FieldSpec.DataType.INT, nullHandling, null);
  }

  protected FluentQueryTest.DeclaringTable givenSingleNullableIntFieldTable(boolean nullHandling,
      @Nullable Consumer<FieldConfig.Builder> customize) {
    return givenSingleNullableFieldTable(FieldSpec.DataType.INT, nullHandling, customize);
  }

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
