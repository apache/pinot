package com.linkedin.thirdeye.detection.algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.ResponseParserUtils;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LegacyAnomalyFunctionAlgorithm extends DetectionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyAnomalyFunctionAlgorithm.class);
  private static String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static String PROP_SPEC = "specs";

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private BaseAnomalyFunction anomalyFunction;

  public LegacyAnomalyFunctionAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_ANOMALY_FUNCTION_CLASS));
    String anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);
    anomalyFunction = (BaseAnomalyFunction) Class.forName(anomalyFunctionClassName).newInstance();
    String specs = OBJECT_MAPPER.writeValueAsString(MapUtils.getMap(config.getProperties(), PROP_SPEC));
    anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    LOG.info("Running legacy anomaly detection for time range {} to {}", this.startTime, this.endTime);

    String datasetName = anomalyFunction.getSpec().getCollection();
    DatasetConfigDTO datasetConfigDTO =
        this.provider.fetchDatasets(Collections.singletonList(datasetName)).get(datasetName);

    Collection<MergedAnomalyResultDTO> historyMergedAnomalies;
    if (anomalyFunction.useHistoryAnomaly() && config.getId() != null) {
      AnomalySlice slice =
          new AnomalySlice().withConfigId(config.getId()).withStart(this.startTime).withEnd(this.endTime);
      historyMergedAnomalies = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);
    } else {
      historyMergedAnomalies = Collections.emptyList();
    }

    Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap =
        AnomalyDetectionInputContextBuilder.getTimeSeriesForAnomalyDetection(anomalyFunction.getSpec(),
            anomalyFunction.getDataRangeIntervals(this.startTime, this.endTime), false);

    Map<DimensionMap, MetricTimeSeries> dimensionMapMetricTimeSeriesMap = new HashMap<>();
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : dimensionKeyMetricTimeSeriesMap.entrySet()) {
      DimensionKey dimensionKey = entry.getKey();

      // If the current time series belongs to OTHER dimension, which consists of time series whose
      // sum of all its values belows 1% of sum of all time series values, then its anomaly is
      // meaningless and hence we don't want to detection anomalies on it.
      String[] dimensionValues = dimensionKey.getDimensionValues();
      boolean isOTHERDimension = false;
      for (String dimensionValue : dimensionValues) {
        if (dimensionValue.equalsIgnoreCase(ResponseParserUtils.OTHER) || dimensionValue.equalsIgnoreCase(
            ResponseParserUtils.UNKNOWN)) {
          isOTHERDimension = true;
          break;
        }
      }
      if (isOTHERDimension) {
        continue;
      }

      DimensionMap dimensionMap = DimensionMap.fromDimensionKey(dimensionKey, datasetConfigDTO.getDimensions());
      dimensionMapMetricTimeSeriesMap.put(dimensionMap, entry.getValue());

      if (entry.getValue().getTimeWindowSet().size() < 1) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", dimensionMap);
      }
    }

    List<AnomalyResult> results = new ArrayList<>();
    for (Map.Entry<DimensionMap, MetricTimeSeries> entry : dimensionMapMetricTimeSeriesMap.entrySet()) {
      try {
        List<AnomalyResult> resultsOfAnEntry =
            anomalyFunction.analyze(entry.getKey(), entry.getValue(), new DateTime(this.startTime),
                new DateTime(this.endTime), new ArrayList<>(historyMergedAnomalies));
        results.addAll(resultsOfAnEntry);

      } catch (Exception ignore) {
        // ignore
      }
    }

    Collection<MergedAnomalyResultDTO> mergedAnomalyResults =
        Collections2.transform(results, new Function<AnomalyResult, MergedAnomalyResultDTO>() {
          @Override
          public MergedAnomalyResultDTO apply(AnomalyResult result) {
            MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
            anomaly.populateFrom(result);
            return anomaly;
          }
        });

    LOG.info("Detected {} anomalies", mergedAnomalyResults.size());
    return new DetectionPipelineResult(new ArrayList<>(mergedAnomalyResults), this.endTime);
  }
}
