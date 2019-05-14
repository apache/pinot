/*
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

package org.apache.pinot.thirdeye.detection.validators;

import com.google.common.base.Preconditions;
import javax.xml.bind.ValidationException;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.quartz.CronExpression;


public class DetectionConfigValidator implements ConfigValidator<DetectionConfigDTO> {

  private final DataProvider provider;
  private final DetectionPipelineLoader loader;

  public DetectionConfigValidator() {
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO,
        DAORegistry.getInstance().getEventDAO(),
        DAORegistry.getInstance().getMergedAnomalyResultDAO(),
        DAORegistry.getInstance().getEvaluationManager(),
        timeseriesLoader, aggregationLoader, loader);
  }

  /**
   * Validate the pipeline by loading and initializing components
   */
  private void semanticValidation(DetectionConfigDTO detectionConfig) throws ValidationException {
    try {
      // backup and swap out id
      Long id = detectionConfig.getId();
      detectionConfig.setId(-1L);

      // try to load the detection pipeline and init all the components
      this.loader.from(provider, detectionConfig, 0, 0);

      // set id back
      detectionConfig.setId(id);
    } catch (Exception e){
      // exception thrown in validate pipeline via reflection
      throw new ValidationException("Semantic error: " + e.getCause().getMessage());
    }
  }

  /**
   * Perform validation on the detection config like verifying if all the required fields are set
   */
  @Override
  public void validateConfig(DetectionConfigDTO detectionConfig) throws ValidationException {
    // Cron Validator
    if (!CronExpression.isValidExpression(detectionConfig.getCron())) {
      throw new ValidationException("The detection cron specified is incorrect. Please verify your cron expression"
          + " using online cron makers.");
    }

    semanticValidation(detectionConfig);
  }

  /**
   * Perform validation on the updated detection config. Check for fields which shouldn't be
   * updated by the user.
   */
  @Override
  public void validateUpdatedConfig(DetectionConfigDTO updatedConfig, DetectionConfigDTO oldConfig)
      throws ValidationException {
    validateConfig(updatedConfig);
    Preconditions.checkArgument(updatedConfig.getId().equals(oldConfig.getId()));
    Preconditions.checkArgument(updatedConfig.getLastTimestamp() == oldConfig.getLastTimestamp());
  }
}
