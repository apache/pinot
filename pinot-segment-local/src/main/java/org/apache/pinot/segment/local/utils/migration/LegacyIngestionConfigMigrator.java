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
package org.apache.pinot.segment.local.utils.migration;

import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.migration.TableConfigMigrator;
import org.apache.pinot.spi.config.table.TableConfig;


/// Migrates a [TableConfig] from migration version 0 (pre-framework) to version 1 by folding the
/// deprecated ingestion-related fields into [org.apache.pinot.spi.config.table.ingestion.IngestionConfig]:
///
/// - `tableIndexConfig.streamConfigs` -> `ingestionConfig.streamIngestionConfig`
/// - `segmentsConfig.segmentPushType` / `segmentPushFrequency` -> `ingestionConfig.batchIngestionConfig`
///
/// The transform delegates to [TableConfigUtils#convertFromLegacyTableConfig(TableConfig)], which
/// mutates the config in place and clears the deprecated fields. Configs that already use the
/// current ingestion shape are left effectively unchanged (the deprecated fields are simply null).
public class LegacyIngestionConfigMigrator implements TableConfigMigrator {

  @Override
  public int fromVersion() {
    return 0;
  }

  @Override
  public TableConfig migrate(TableConfig input) {
    TableConfigUtils.convertFromLegacyTableConfig(input);
    return input;
  }
}
