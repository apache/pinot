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

package org.apache.pinot.thirdeye.detection.finetune;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GridSearchTuningAlgorithm implements TuningAlgorithm {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(GridSearchTuningAlgorithm.class);
  private final String jsonProperties;
  private final LinkedHashMap<String, List<Number>> parameters;
  private final DetectionPipelineLoader loader;
  private final DataProvider provider;
  private final MergedAnomalyResultManager anomalyDAO;
  private Map<DetectionConfigDTO, Double> scores;
  private Map<DetectionConfigDTO, DetectionPipelineResult> results;
  private ScoreFunction scoreFunction;

  /**
   * Instantiates a new Grid search tuning algorithm.
   *
   * @param jsonProperties the json properties for detection config
   * @param parameters the parameters to tune. LinkedHashMap from json path to values to try
   *
   */
  public GridSearchTuningAlgorithm(String jsonProperties, LinkedHashMap<String, List<Number>> parameters) {
    this.jsonProperties = jsonProperties;
    this.parameters = parameters;
    this.loader = new DetectionPipelineLoader();

    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    EventManager eventDAO = DAORegistry.getInstance().getEventDAO();
    EvaluationManager evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(), ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, loader, TimeSeriesCacheBuilder.getInstance(),
        AnomaliesCacheBuilder.getInstance());
    this.scores = new HashMap<>();
    this.results = new LinkedHashMap<>();
    this.scoreFunction = new TimeBucketF1ScoreFunction();
  }

  /**
   * Fit into a time range and evaluate the best config.
   *
   * @param slice anomaly slice
   * @throws Exception the exception
   */
  @Override
  public void fit(AnomalySlice slice, long configId) throws Exception {
    slice = slice.withDetectionId(configId);
    Collection<MergedAnomalyResultDTO> testAnomalies = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);
    fit(slice, new HashMap<String, Number>(), testAnomalies);
  }

  /**
   * Fit into a time range and evaluate the best config recursively.
   * */
  private void fit(AnomalySlice slice, Map<String, Number> currentParameters,
      Collection<MergedAnomalyResultDTO> testAnomalies) throws Exception {
    if (currentParameters.size() == this.parameters.size()) {
      DetectionConfigDTO config = makeConfigFromParameters(currentParameters);
      DetectionPipeline pipeline = this.loader.from(this.provider, config, slice.getStart(), slice.getEnd());
      DetectionPipelineResult result = pipeline.run();
      this.results.put(config, result);
      // calculate score
      this.scores.put(config, this.scoreFunction.calculateScore(result, testAnomalies));
      LOG.info("Score for detection config {} is {}", OBJECT_MAPPER.writeValueAsString(config), this.scores.get(config));
      return;
    }
    String path = Iterables.get(this.parameters.keySet(), currentParameters.size());
    List<Number> values = this.parameters.get(path);
    for (Number value : values) {
      currentParameters.put(path, value);
      fit(slice, currentParameters, testAnomalies);
      currentParameters.remove(path);
    }
  }

  private DetectionConfigDTO makeConfigFromParameters(Map<String, Number> currentParameters) throws IOException {
    DocumentContext jsonContext = JsonPath.parse(this.jsonProperties);
    // Replace parameters using json path
    for (Map.Entry<String, Number> entry : currentParameters.entrySet()) {
      String path = entry.getKey();
      Number value = entry.getValue();
      jsonContext.set(path, value);
    }
    Map<String, Object> properties = OBJECT_MAPPER.readValue(jsonContext.jsonString(), Map.class);
    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setId(Long.MAX_VALUE);
    config.setName("preview");
    config.setDescription("previewing the detection");
    config.setProperties(properties);

    return config;
  }

  /**
   * Get the best detection config detection config in this grid search.
   *
   * @return the detection config dto
   */
  @Override
  public DetectionConfigDTO bestDetectionConfig() {
    // returns the detection config with the highest score
    double maxVal = -1;
    DetectionConfigDTO bestConfig = null;
    for (Map.Entry<DetectionConfigDTO, Double> entry : this.scores.entrySet()) {
      if (entry.getValue() > maxVal) {
        bestConfig = entry.getKey();
        maxVal = entry.getValue();
      }
    }
    if (bestConfig != null) {
      try {
        LOG.info("Best detection config found is {} with score {}", OBJECT_MAPPER.writeValueAsString(bestConfig), this.scores.get(bestConfig));
      } catch (JsonProcessingException e) {
        LOG.error("error processing json config", e);
      }
      return bestConfig;
    }

    // if no scores is available, compare the number of anomalies detected by each config.
    if (!this.results.isEmpty()) {
      int maxNumberOfAnomalies = 0;
      bestConfig = (DetectionConfigDTO) results.keySet().toArray()[0];
      for (Map.Entry<DetectionConfigDTO, DetectionPipelineResult> entry : results.entrySet()) {
        if (entry.getValue().getAnomalies().size() > maxNumberOfAnomalies) {
          bestConfig = entry.getKey();
          maxNumberOfAnomalies = entry.getValue().getAnomalies().size();
        }
      }
    }
    return bestConfig;
  }
}
