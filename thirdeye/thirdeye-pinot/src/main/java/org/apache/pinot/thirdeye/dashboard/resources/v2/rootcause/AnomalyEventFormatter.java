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

package org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.resources.v2.ResourceUtils;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.rootcause.impl.AnomalyEventEntity;
import org.apache.pinot.thirdeye.rootcause.impl.EventEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;


public class AnomalyEventFormatter extends RootCauseEventEntityFormatter {
  public static final String ATTR_DATASET = "dataset";
  public static final String ATTR_METRIC = "metric";
  public static final String ATTR_METRIC_ID = "metricId";
  public static final String ATTR_FUNCTION = "function";
  public static final String ATTR_FUNCTION_ID = "functionId";
  public static final String ATTR_CURRENT = "current";
  public static final String ATTR_BASELINE = "baseline";
  public static final String ATTR_STATUS = "status";
  public static final String ATTR_ISSUE_TYPE = "issueType";
  public static final String ATTR_DIMENSIONS = "dimensions";
  public static final String ATTR_COMMENT = "comment";
  public static final String ATTR_EXTERNAL_URLS = "externalUrls";
  public static final String ATTR_WEIGHT = "weight";
  public static final String ATTR_SCORE = "score";
  public static final String ATTR_METRIC_GRANULARITY = "metricGranularity";
  public static final String ATTR_STATUS_CLASSIFICATION = "statusClassification";
  public static final String ATTR_AGGREGATE_MULTIPLIER = "aggregateMultiplier";
  public static final String ATTR_DETECTION_ID = "detectionConfigId";

  private final MergedAnomalyResultManager anomalyDAO;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final DetectionConfigManager detectionDAO;

  public AnomalyEventFormatter() {
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
  }

  public AnomalyEventFormatter(MergedAnomalyResultManager anomalyDAO, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      DetectionConfigManager detectionDAO) {
    this.anomalyDAO = anomalyDAO;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.detectionDAO = detectionDAO;
  }

  @Override
  public boolean applies(EventEntity entity) {
    return entity instanceof AnomalyEventEntity;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    AnomalyEventEntity e = (AnomalyEventEntity) entity;

    MergedAnomalyResultDTO anomaly = this.anomalyDAO.findById(e.getId());
    Multimap<String, String> attributes = ArrayListMultimap.create();

    MetricConfigDTO metric = getMetricByName(anomaly.getMetric(), anomaly.getCollection());
    Preconditions.checkNotNull(metric);

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    String functionName = "unknown";
    Long detectionConfigId = null;

    if (anomaly.getDetectionConfigId() != null){
      DetectionConfigDTO detectionConfigDTO = detectionDAO.findById(anomaly.getDetectionConfigId());
      if (detectionConfigDTO == null){
        throw new IllegalArgumentException(String.format("could not resolve detection config id %d", anomaly.getDetectionConfigId()));
      }
      functionName = detectionConfigDTO.getName();
      detectionConfigId = anomaly.getDetectionConfigId();
    }

    if (anomaly.getFunctionId() != null){
      AnomalyFunctionDTO function = anomaly.getFunction();
      functionName = function.getFunctionName();
      attributes.put(ATTR_FUNCTION_ID, String.valueOf(function.getId()));
      attributes.put(ATTR_AGGREGATE_MULTIPLIER, String.valueOf(getAggregateMultiplier(anomaly, dataset, metric)));
    }

    attributes.put(ATTR_FUNCTION, functionName);
    attributes.put(ATTR_DETECTION_ID, String.valueOf(detectionConfigId));

    String comment = "";
    AnomalyFeedbackType status = AnomalyFeedbackType.NO_FEEDBACK;
    if (anomaly.getFeedback() != null) {
      comment = anomaly.getFeedback().getComment();
      status = anomaly.getFeedback().getFeedbackType();
    }

    Map<String, String> externalUrls = ResourceUtils.getExternalURLs(anomaly, this.metricDAO, this.datasetDAO);

    attributes.put(ATTR_DATASET, anomaly.getCollection());
    attributes.put(ATTR_METRIC, anomaly.getMetric());
    attributes.put(ATTR_METRIC_ID, String.valueOf(metric.getId()));
    attributes.put(ATTR_CURRENT, String.valueOf(anomaly.getAvgCurrentVal()));
    attributes.put(ATTR_BASELINE, String.valueOf(anomaly.getAvgBaselineVal()));
    attributes.put(ATTR_STATUS, status.toString());
    attributes.put(ATTR_COMMENT, comment);
    // attributes.put(ATTR_ISSUE_TYPE, null); // TODO
    attributes.putAll(ATTR_EXTERNAL_URLS, externalUrls.keySet());
    attributes.put(ATTR_SCORE, String.valueOf(anomaly.getScore()));
    attributes.put(ATTR_WEIGHT, String.valueOf(anomaly.getWeight()));
    attributes.put(ATTR_METRIC_GRANULARITY, dataset.bucketTimeGranularity().toAggregationGranularityString());
    attributes.put(ATTR_STATUS_CLASSIFICATION, ResourceUtils.getStatusClassification(anomaly).toString());

    // external urls as attributes
    for (Map.Entry<String, String> entry : externalUrls.entrySet()) {
      attributes.put(entry.getKey(), entry.getValue());
    }

    // dimensions as attributes and label
    Multimap<String, String> filters = MetricEntity.fromURN(anomaly.getMetricUrn()).getFilters();

    List<String> dimensionStrings = new ArrayList<>();
    for (Map.Entry<String, String> entry : filters.entries()) {
      dimensionStrings.add(entry.getValue());
      attributes.put(entry.getKey(), entry.getValue());

      if (!attributes.containsEntry(ATTR_DIMENSIONS, entry.getKey())) {
        attributes.put(ATTR_DIMENSIONS, entry.getKey());
      }
    }

    String dimensionString = "";
    if (!dimensionStrings.isEmpty()) {
      dimensionString = String.format(" (%s)", StringUtils.join(dimensionStrings, ", "));
    }

    String label = String.format("%s%s", functionName, dimensionString);
    String link = String.format("#/rootcause?anomalyId=%d", anomaly.getId());

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, anomaly.getStartTime(), getAdjustedEndTime(anomaly), null);
    out.setAttributes(attributes);

    return out;
  }

  private MetricConfigDTO getMetricByName(String metric, String dataset) {
    MetricConfigDTO metricDTO = this.metricDAO.findByMetricAndDataset(metric, dataset);
    if (metricDTO == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric '%s' in dataset '%s'", metric, dataset));
    }
    return metricDTO;
  }

  /**
   * (Business logic) Returns the multiplier for range-aggregates to be used for display on RCA header
   *
   * @param anomaly anomaly
   * @param dataset dataset config
   * @param metric metric config
   * @return multiplier between {@code 0.0} and {@code 1.0}
   */
  private double getAggregateMultiplier(MergedAnomalyResultDTO anomaly, DatasetConfigDTO dataset, MetricConfigDTO metric) {
    if (MetricAggFunction.SUM.equals(metric.getDefaultAggFunction())
        || MetricAggFunction.COUNT.equals(metric.getDefaultAggFunction())) {
      return dataset.bucketTimeGranularity().toMillis() / (double) (anomaly.getEndTime() - anomaly.getStartTime());

    } else {
      return 1.0;
    }
  }

  /**
   * (Business logic) Returns the end time of an anomaly rounded up to a full minute.
   *
   * @param anomaly anomaly
   * @return adjusted timestamp (in millis) rounded to full seconds
   */
  private static long getAdjustedEndTime(MergedAnomalyResultDTO anomaly) {
    if (anomaly.getEndTime() % 60000 == 0) {
      return anomaly.getEndTime();
    }
    return (long) Math.floor((anomaly.getEndTime() + 59999) / 60000.0) * 60000;
  }
}
