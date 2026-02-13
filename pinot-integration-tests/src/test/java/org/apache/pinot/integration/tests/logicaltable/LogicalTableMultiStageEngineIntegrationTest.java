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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.integration.tests.MultiStageEngineIntegrationTest;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;



public class LogicalTableMultiStageEngineIntegrationTest extends MultiStageEngineIntegrationTest {

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    initCluster();
    List<File> avroFiles = unpackAvroData(_tempDir);
    initTables(avroFiles);
    initOtherDependencies(avroFiles);
  }

  @Override
  public void initTables(List<File> avroFiles)
      throws Exception {
    uploadDataToOfflineTables(getOfflineTablesCreated(), avroFiles);
    createLogicalTableAndSchema();
  }

  @Override
  protected List<String> getOfflineTablesCreated() {
    return List.of("physicalTable_0", "physicalTable_1");
  }

  @Override
  protected String getLogicalTableName() {
    return getTableName();
  }

  @Override
  protected LogicalTableConfig createLogicalTableConfig() {
    List<String> offlineTableNames = getOfflineTablesCreated().stream().map(TableNameBuilder.OFFLINE::tableNameWithType)
        .collect(Collectors.toList());
    return createLogicalTableConfig(offlineTableNames, List.of());
  }

  @DataProvider(name = "polymorphicScalarComparisonFunctionsDataProvider")
  public Object[][] polymorphicScalarComparisonFunctionsDataProvider() {
    return super.polymorphicScalarComparisonFunctionsDataProvider();
  }

  @AfterClass
  @Override
  public void tearDown()
      throws Exception {
    dropLogicalTable(getTableName());
    super.tearDown();
  }

  @Override
  @Test
  public void testBetween()
      throws Exception {
    testBetween(false);
    // TODO - Explain plan result is different for logical table vs physical table.
    //  That is why we're overriding the test and avoiding explain plan validation.
    //  This needs to be fixed
  }

  // These validate tests are applicable only for physical table and not logical table, so ignoring them here.
  // These tests get the table config of the physical table and send it in the request,
  // which doesn't work for logical table.
  @Override
  @Ignore
  public void testValidateQueryApiBatchMixedResults() {
  }

  @Override
  @Ignore
  public void testValidateQueryApiSuccessfulQueries() {
  }

  @Override
  @Ignore
  public void testValidateQueryApiUnsuccessfulQueries() {
  }

  @Override
  @Ignore
  public void testValidateQueryApiWithIgnoreCaseOption() {
  }
}
