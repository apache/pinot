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
package org.apache.pinot.core.query.aggregation;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
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

/** query-based tests for groupby-orderby */
public class GroupByOrderByTest {
  @Test
  public void testSingleTableWithNullHandlingEnabled()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(
            new PinotConfiguration(Map.of(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest(
            "SET enableNullHandling=true; SELECT col1 FROM testTable ORDER BY col1");
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    List<Object[]> unsortedRows = new ArrayList<>();
    unsortedRows.add(new Object[]{1});
    unsortedRows.add(new Object[]{2});
    unsortedRows.add(new Object[]{3});
    unsortedRows.add(new Object[]{4});

    DataTable dataTable = SelectionOperatorUtils.getDataTableFromRows(unsortedRows, dataSchema, false);
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    int numSortedInstances = 1;
    for (int i = 0; i < numSortedInstances; i++) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", i, TableType.OFFLINE);
      dataTableMap.put(instance, dataTable);
    }
    long reduceTimeoutMs = 100000;
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, reduceTimeoutMs,
            mock(BrokerMetrics.class));
    brokerReduceService.shutDown();

    ResultTable resultTable = brokerResponse.getResultTable();
  }

  @Test
  public void list() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5},
            new Object[]{6},
            new Object[]{7}
        )
        .andOnSecondInstance(
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5},
            new Object[]{6},
            new Object[]{7},
            new Object[]{8}
        )
        .whenQuery("select COUNT(1) from testTable group by myField order by myField limit 1")
        .thenResultIs("LONG",
            "1"
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
