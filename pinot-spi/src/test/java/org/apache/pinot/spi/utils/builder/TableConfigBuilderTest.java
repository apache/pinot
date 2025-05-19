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

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the validations in {@link TableConfigBuilder}
 */
public class TableConfigBuilderTest {

  private static final String TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "timeColumn";

  @Test
  public void testValidateSkipSegmentPreprocessFlag() {

    TableConfig tableconfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN)
        .setSkipSegmentPreprocess(true).build();
    Assert.assertTrue(tableconfig.getIndexingConfig().isSkipSegmentPreprocess(),
        "skipSegmentPreprocess will be true");
  }
}
