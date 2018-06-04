package com.linkedin.thirdeye.detection.finetune;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.DefaultAggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultDataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class GridSearchTuningAlgorithm implements TuningAlgorithm {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.provider =
        new DefaultDataProvider(metricDAO, eventDAO, anomalyDAO, timeseriesLoader, aggregationLoader, loader);
    this.scores = new HashMap<>();
    this.results = new LinkedHashMap<>();
    this.scoreFunction = new F1ScoreFunction();
  }

  /**
   * Fit into a time range and evaluate the best config.
   *
   * @param start the start
   * @param end the end
   * @throws Exception the exception
   */
  @Override
  public void fit(long start, long end) throws Exception {
    List<MergedAnomalyResultDTO> testAnomalies = this.anomalyDAO.findByTime(start, end);
    fit(start, end, new HashMap<String, Number>(), testAnomalies);
  }

  /**
   * Fit into a time range and evaluate the best config recursively.
   * */
  private void fit(long start, long end, Map<String, Number> currentParameters,
      List<MergedAnomalyResultDTO> testAnomalies) throws Exception {
    if (currentParameters.size() == this.parameters.size()) {
      DetectionConfigDTO config = makeConfigFromParameters(currentParameters);
      DetectionPipeline pipeline = this.loader.from(this.provider, config, start, end);
      DetectionPipelineResult result = pipeline.run();
      this.results.put(config, result);
      // calculate score
      this.scores.put(config, this.scoreFunction.calculateScore(result, testAnomalies));
      return;
    }
    String path = Iterables.get(this.parameters.keySet(), currentParameters.size());
    List<Number> values = this.parameters.get(path);
    for (Number value : values) {
      currentParameters.put(path, value);
      fit(start, end, currentParameters, testAnomalies);
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
    // if no scores is available, compare the number of anomalies detected by each config.
    if (bestConfig == null && !this.results.isEmpty()) {
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
