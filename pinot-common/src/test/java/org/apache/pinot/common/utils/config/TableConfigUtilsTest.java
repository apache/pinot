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
package org.apache.pinot.common.utils.config;

import java.util.Collections;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;


public class TableConfigUtilsTest {

  @Test
  public void testValidate() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

    // Default table config
    TableConfigUtils.validate(tableConfig);

    // Invalid field config (dictionary encoding & text index)
    try {
      FieldConfig textFieldConfig =
          new FieldConfig("text", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null);
      tableConfig.setFieldConfigList(Collections.singletonList(textFieldConfig));
      TableConfigUtils.validate(tableConfig);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Valid field config (no-dictionary & text index) but raw index not configured in indexing config
    try {
      FieldConfig textFieldConfig =
          new FieldConfig("text", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null);
      tableConfig.setFieldConfigList(Collections.singletonList(textFieldConfig));
      TableConfigUtils.validate(tableConfig);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Valid field config (no-dictionary & text index) and indexing config
    tableConfig.getIndexingConfig().setNoDictionaryColumns(Collections.singletonList("text"));
    TableConfigUtils.validate(tableConfig);
  }
}
