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

import java.util.List;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


// FastFilteredCountTest, but using multi-col text index
public class FastFilteredCountMCTest extends FastFilteredCountTest {
  protected TableConfig getTableConfig() {
    // use multi-column index instead
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig.Builder(TEXT_COLUMN)
            .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
            .build());

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(CLASSIFICATION_COLUMN, SORTED_COLUMN))
        .setJsonIndexColumns(List.of(JSON_COLUMN))
        .setRangeIndexColumns(List.of(INT_RANGE_COLUMN))
        .setFieldConfigList(fieldConfigs)
        .setMultiColumnTextIndexConfig(new MultiColumnTextIndexConfig(List.of(TEXT_COLUMN)))
        .build();
    return tableConfig;
  }
}
