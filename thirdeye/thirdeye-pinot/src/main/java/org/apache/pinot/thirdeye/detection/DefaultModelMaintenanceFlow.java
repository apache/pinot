/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.apache.pinot.thirdeye.detection;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.MapeAveragePercentageChangeModelEvaluator;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;
import org.apache.pinot.thirdeye.detection.spec.MapeAveragePercentageChangeModelEvaluatorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.ModelEvaluator;
import org.apache.pinot.thirdeye.detection.spi.model.ModelStatus;
import org.apache.pinot.thirdeye.detection.yaml.DetectionConfigTuner;
import org.joda.time.Instant;


/**
 * The default model maintenance flow. If the model is tunable, this flow will run the configured model evaluators for
 * the detection config and automatically re-tunes the model.
 */
public class DefaultModelMaintenanceFlow implements ModelMaintenanceFlow {
  private static int DEFAULT_TUNING_WINDOW_DAYS = 28;

  private final DataProvider provider;
  private final DetectionRegistry detectionRegistry;

  DefaultModelMaintenanceFlow(DataProvider provider, DetectionRegistry detectionRegistry) {
    this.provider = provider;
    this.detectionRegistry = detectionRegistry;
  }

  public DetectionConfigDTO maintain(DetectionConfigDTO config, Instant timestamp) {
    // if the pipeline is tunable, get the model evaluators
    if (isTunable(config)) {
      Collection<? extends ModelEvaluator<? extends AbstractSpec>> modelEvaluators = getModelEvaluators(config);
      for (ModelEvaluator<? extends AbstractSpec> modelEvaluator : modelEvaluators) {
        if (modelEvaluator.evaluateModel(timestamp).getStatus().equals(ModelStatus.BAD)) {
          DetectionConfigTuner detectionConfigTuner = new DetectionConfigTuner(config, provider);
          config = detectionConfigTuner.tune(timestamp.toDateTime().minusDays(DEFAULT_TUNING_WINDOW_DAYS).getMillis(),
              timestamp.getMillis());
          config.setLastTuningTimestamp(timestamp.getMillis());
          break;
        }
      }
    }
    return config;
  }

  private Collection<? extends ModelEvaluator<? extends AbstractSpec>> getModelEvaluators(DetectionConfigDTO config) {
    // get the model evaluator in the detection config
    Collection<? extends ModelEvaluator<? extends AbstractSpec>> modelEvaluators = config.getComponents()
        .values()
        .stream()
        .filter(component -> component instanceof ModelEvaluator)
        .map(component -> (ModelEvaluator<? extends AbstractSpec>) component)
        .collect(Collectors.toList());

    if (modelEvaluators.isEmpty()) {
      // if evaluators are not configured, use the default ones
      modelEvaluators = instantiateDefaultEvaluators(config);
    }

    return modelEvaluators;
  }

  private Collection<ModelEvaluator<MapeAveragePercentageChangeModelEvaluatorSpec>> instantiateDefaultEvaluators(
      DetectionConfigDTO config) {
    ModelEvaluator<MapeAveragePercentageChangeModelEvaluatorSpec> evaluator =
        new MapeAveragePercentageChangeModelEvaluator();
    evaluator.init(new MapeAveragePercentageChangeModelEvaluatorSpec(),
        new DefaultInputDataFetcher(this.provider, config.getId()));
    return Collections.singleton(evaluator);
  }

  /**
   * If the detection config contains a tunable component
   * @param configDTO the detection config
   * @return True if the detection config is contains tunable component
   */
  private boolean isTunable(DetectionConfigDTO configDTO) {
    return configDTO.getComponents()
        .values()
        .stream()
        .anyMatch(component -> this.detectionRegistry.isTunable(component.getClass().getName()));
  }
}
