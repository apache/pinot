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
package org.apache.pinot.controller.tuner;

import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Used to auto-tune the table indexing config. It takes the original table
 * config, table schema and adds the following to indexing config:
 * - Inverted indices for all dimensions
 * - No dictionary index for all metrics
 */
@Tuner(name = "realtimeAutoIndexTuner")
public class RealTimeAutoIndexTuner implements TableConfigTuner {

  @Override
  public TableConfig apply(PinotHelixResourceManager pinotHelixResourceManager,
      TableConfig tableConfig, Schema schema, Map<String, String> extraProperties) {
    IndexingConfig initialIndexingConfig = tableConfig.getIndexingConfig();
    initialIndexingConfig.setInvertedIndexColumns(schema.getDimensionNames());
    initialIndexingConfig.setNoDictionaryColumns(schema.getMetricNames());
    return tableConfig;
  }
}
