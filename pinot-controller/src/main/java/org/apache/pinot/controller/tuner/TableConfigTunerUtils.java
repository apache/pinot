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

import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableConfigTunerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigTunerUtils.class);

  private TableConfigTunerUtils() {
  }

  /**
   * Apply the entire tunerConfig list inside the tableConfig.
   */
  public static void applyTunerConfigs(PinotHelixResourceManager pinotHelixResourceManager, TableConfig tableConfig,
      Schema schema, Map<String, String> extraProperties) {
    List<TunerConfig> tunerConfigs = tableConfig.getTunerConfigsList();
    if (CollectionUtils.isNotEmpty(tunerConfigs)) {
      for (TunerConfig tunerConfig : tunerConfigs) {
        applyTunerConfig(pinotHelixResourceManager, tunerConfig, tableConfig, schema, extraProperties);
      }
    }
  }

  /**
   * Apply one explicit tunerConfig to the tableConfig
   */
  public static void applyTunerConfig(
      PinotHelixResourceManager pinotHelixResourceManager, TunerConfig tunerConfig, TableConfig tableConfig,
      Schema schema, Map<String, String> extraProperties) {
    if (tunerConfig != null && tunerConfig.getName() != null && !tunerConfig.getName().isEmpty()) {
      try {
        TableConfigTuner tuner = TableConfigTunerRegistry.getTuner(tunerConfig.getName());
        tuner.apply(pinotHelixResourceManager, tableConfig, schema, extraProperties);
      } catch (Exception e) {
        LOGGER.error(String.format("Exception occur when applying tuner: %s", tunerConfig.getName()), e);
      }
    }
  }
}
