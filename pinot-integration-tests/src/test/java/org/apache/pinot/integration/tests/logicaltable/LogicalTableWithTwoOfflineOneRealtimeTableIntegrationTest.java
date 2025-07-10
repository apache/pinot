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
package org.apache.pinot.integration.tests.logicaltable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.query.timeboundary.TimeBoundaryStrategy;
import org.apache.pinot.query.timeboundary.TimeBoundaryStrategyService;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;


public class LogicalTableWithTwoOfflineOneRealtimeTableIntegrationTest extends BaseLogicalTableIntegrationTest {
  @Override
  protected List<String> getOfflineTableNames() {
    return List.of("o_1", "o_2");
  }

  @Override
  protected List<String> getRealtimeTableNames() {
    return List.of("r_1");
  }

  @Override
  protected Map<String, List<File>> getRealtimeTableDataFiles() {
    // Overlapping data files for the hybrid table
    return distributeFilesToTables(getRealtimeTableNames(),
        _avroFiles.subList(_avroFiles.size() - 4, _avroFiles.size()));
  }

  @Test
  public void testUpdateLogicalTableTimeBoundary()
      throws Exception {
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    updateTimeBoundaryTableInLogicalTable(logicalTableConfig);

    // Wait to ensure logical table config update helix message is processed in broker
    waitForAllDocsLoaded(5_000);

    // Run the tests
    testGeneratedQueries();
    testHardcodedQueries();
    testQueriesFromQueryFile();
  }

  private void updateTimeBoundaryTableInLogicalTable(LogicalTableConfig logicalTableConfig)
      throws IOException {
    TimeBoundaryStrategy timeBoundaryStrategy = TimeBoundaryStrategyService.getInstance()
        .getTimeBoundaryStrategy(logicalTableConfig.getTimeBoundaryConfig().getBoundaryStrategy());
    List<String> includedTables = timeBoundaryStrategy.getTimeBoundaryTableNames(logicalTableConfig);

    String timeBoundaryTableName = TableNameBuilder.extractRawTableName(includedTables.get(0));
    String newTimeBoundaryTableName = timeBoundaryTableName.equals("o_1") ? "o_2" : "o_1";
    newTimeBoundaryTableName = TableNameBuilder.OFFLINE.tableNameWithType(newTimeBoundaryTableName);

    Map<String, Object> parameters = Map.of("includedTables", List.of(newTimeBoundaryTableName));
    logicalTableConfig.getTimeBoundaryConfig().setParameters(parameters);
    logicalTableConfig.setQueryConfig(null);

    updateLogicalTableConfig(logicalTableConfig);
  }
}
