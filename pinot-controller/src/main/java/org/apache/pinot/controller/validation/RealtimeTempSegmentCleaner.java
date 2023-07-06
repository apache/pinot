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
package org.apache.pinot.controller.validation;

import java.util.List;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeTempSegmentCleaner extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentValidationManager.class);

  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final ValidationMetrics _validationMetrics;

  public RealtimeTempSegmentCleaner(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ValidationMetrics validationMetrics, ControllerMetrics controllerMetrics) {
    super("RealtimeTempSegmentCleaner", config.getRealtimeTmpSegmentCleanupFrequencyInSeconds(),
        config.getRealtimeTmpSegmentCleanupInitialDelaySeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _validationMetrics = validationMetrics;
  }

  @Override
  protected void processTable(String tableNameWithType) {
    long tmpSegments = _llcRealtimeSegmentManager.deleteTmpSegments(tableNameWithType);
    _validationMetrics.updateTmpSegCountGauge(tableNameWithType, tmpSegments);
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
        _validationMetrics.cleanupTmpSegCountGauge(tableNameWithType);
      }
    }
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Unregister all the validation metrics.");
    _validationMetrics.unregisterAllMetrics();
  }
}
