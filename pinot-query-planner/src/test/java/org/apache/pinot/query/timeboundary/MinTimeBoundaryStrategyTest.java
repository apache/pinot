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
package org.apache.pinot.query.timeboundary;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;


public class MinTimeBoundaryStrategyTest {

  TableCache _mockTableCache;
  RoutingManager _mockRoutingManager;
  TimeBoundaryStrategy _minTimeBoundaryStrategy = new MinTimeBoundaryStrategy();

  public void setupMocks(Map<String, TimeBoundaryInfo> data) {
    for (String tableName : data.keySet()) {
      if (TableNameBuilder.isRealtimeTableResource(tableName)) {
        continue;
      }
      TimeBoundaryInfo timeBoundaryInfo = data.get(tableName);
      when(_mockRoutingManager.getTimeBoundaryInfo(tableName)).thenReturn(timeBoundaryInfo);
      Schema schema = mock(Schema.class);
      TableConfig tableConfig = mock(TableConfig.class);
      SegmentsValidationAndRetentionConfig validationMock = mock(SegmentsValidationAndRetentionConfig.class);
      DateTimeFieldSpec dateTimeFieldSpec = mock(DateTimeFieldSpec.class);
      DateTimeFormatSpec dateTimeFormatSpec = mock(DateTimeFormatSpec.class);

      when(_mockTableCache.getSchema(TableNameBuilder.extractRawTableName(tableName))).thenReturn(schema);
      when(_mockTableCache.getTableConfig(tableName)).thenReturn(tableConfig);
      when(tableConfig.getValidationConfig()).thenReturn(validationMock);
      when(validationMock.getTimeColumnName()).thenReturn(timeBoundaryInfo.getTimeColumn());
      when(schema.getSpecForTimeColumn(timeBoundaryInfo.getTimeColumn())).thenReturn(dateTimeFieldSpec);
      when(dateTimeFieldSpec.getFormatSpec()).thenReturn(dateTimeFormatSpec);
      when(dateTimeFormatSpec.fromFormatToMillis(any())).thenReturn(Long.valueOf(timeBoundaryInfo.getTimeValue()));
    }
  }


  @DataProvider
  public Object[][] timeBoundaryData() {
    Map<String, TimeBoundaryInfo> timeBoundaryInfoMap = Map.of(
        "table1_OFFLINE", new TimeBoundaryInfo("timeColumn1", "1747134822000"),
        "table2_OFFLINE", new TimeBoundaryInfo("timeColumn2", "1747134844000"),
        "table3_OFFLINE", new TimeBoundaryInfo("timeColumn3", "1747134866000"),
        "table4_OFFLINE", new TimeBoundaryInfo("timeColumn4", "1747134888000"),
        "table5_REALTIME", new TimeBoundaryInfo("timeColumn5", "1747134900000")
    );

    return new Object[][]{
        {timeBoundaryInfoMap, List.of("table3_OFFLINE"), "table3_OFFLINE"},
        {timeBoundaryInfoMap, List.of("table2_OFFLINE", "table3_OFFLINE"), "table2_OFFLINE"},
        {timeBoundaryInfoMap, List.of("table3_OFFLINE", "table2_OFFLINE", "table4_OFFLINE"), "table2_OFFLINE"},
        {timeBoundaryInfoMap, List.of(), "empty_includedTables_OFFLINE"}
    };
  }


  @Test(dataProvider = "timeBoundaryData")
  public void testComputeTimeBoundary(Map<String, TimeBoundaryInfo> timeBoundaryInfoMap,
      List<String> includedTables, String expectedTableName) {
    Map<String, Object> parameters = Map.of("includedTables", includedTables);
    testComputeTimeBoundary(timeBoundaryInfoMap, expectedTableName, parameters);
  }

  private void testComputeTimeBoundary(Map<String, TimeBoundaryInfo> timeBoundaryInfoMap, String expectedTableName,
      Map<String, Object> parameters) {
    setupMocks(timeBoundaryInfoMap);
    _minTimeBoundaryStrategy.init(createLogicalTableConfig(parameters), _mockTableCache);
    TimeBoundaryInfo timeBoundaryInfo = _minTimeBoundaryStrategy.computeTimeBoundary(_mockRoutingManager);
    assertSame(timeBoundaryInfo, timeBoundaryInfoMap.get(expectedTableName));
  }

  @BeforeMethod
  public void setUp() {
    _mockTableCache = mock(TableCache.class);
    _mockRoutingManager = mock(RoutingManager.class);
  }

  private LogicalTableConfig createLogicalTableConfig(Map<String, Object> parameters) {
    LogicalTableConfigBuilder builder = new LogicalTableConfigBuilder()
        .setTableName("logical_table")
        .setTimeBoundaryConfig(new TimeBoundaryConfig("min", parameters));
    return builder.build();
  }
}
