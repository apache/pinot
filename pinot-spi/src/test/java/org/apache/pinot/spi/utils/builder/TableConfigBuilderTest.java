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

package org.apache.pinot.spi.utils.builder;

import java.io.IOException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;


/// Tests for the validations in [TableConfigBuilder]
public class TableConfigBuilderTest {

  private static final String TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "timeColumn";

  @DataProvider
  public Object[][] tableTypes() {
    return new Object[][]{{TableType.OFFLINE}, {TableType.REALTIME}};
  }

  @Test(dataProvider = "tableTypes")
  public void testRetentionSizeRoundTrip(TableType tableType)
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(tableType).setTableName(TABLE_NAME)
        .setRetentionSize("100G")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();
    assertEquals(tableConfig.getValidationConfig().getRetentionSize(), "100G");
    assertEquals(tableConfig.toJsonNode().get("segmentsConfig").get("retentionSize").asText(), "100G");

    TableConfig roundTrip = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
    assertEquals(roundTrip, tableConfig);
    assertEquals(roundTrip.getValidationConfig().getRetentionSize(), "100G");
    assertEquals(roundTrip.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    assertEquals(roundTrip.getValidationConfig().getRetentionTimeValue(), "7");
    assertEquals(new TableConfig(tableConfig).getValidationConfig().getRetentionSize(), "100G");
  }

  @Test(dataProvider = "tableTypes")
  public void testRetentionSizeDefaultsToDisabled(TableType tableType)
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(tableType).setTableName(TABLE_NAME).build();
    assertNull(tableConfig.getValidationConfig().getRetentionSize());
    assertFalse(tableConfig.toJsonNode().get("segmentsConfig").has("retentionSize"));
    TableConfig roundTrip = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
    assertNull(roundTrip.getValidationConfig().getRetentionSize());
  }

  @Test
  public void testValidateSkipSegmentPreprocessFlag() {

    TableConfig tableconfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN)
        .setSkipSegmentPreprocess(true).build();
    Assert.assertTrue(tableconfig.getIndexingConfig().isSkipSegmentPreprocess(),
        "skipSegmentPreprocess will be true");
  }
}
