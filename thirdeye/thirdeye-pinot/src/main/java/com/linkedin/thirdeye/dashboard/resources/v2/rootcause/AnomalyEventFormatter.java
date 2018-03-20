package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.dashboard.resources.v2.ResourceUtils;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.impl.AnomalyEventEntity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;


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

  private final MergedAnomalyResultManager anomalyDAO;
  private final MetricConfigManager metricDAO;

  public AnomalyEventFormatter() {
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
  }

  public AnomalyEventFormatter(MergedAnomalyResultManager anomalyDAO, MetricConfigManager metricDAO) {
    this.anomalyDAO = anomalyDAO;
    this.metricDAO = metricDAO;
  }

  @Override
  public boolean applies(EventEntity entity) {
    return entity instanceof AnomalyEventEntity;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    AnomalyEventEntity e = (AnomalyEventEntity) entity;

    MergedAnomalyResultDTO anomaly = this.anomalyDAO.findById(e.getId());
    AnomalyFunctionDTO function = anomaly.getFunction();
    MetricConfigDTO metric = this.getMetricFromFunction(function);

    String comment = "";
    AnomalyFeedbackType status = AnomalyFeedbackType.NO_FEEDBACK;
    if (anomaly.getFeedback() != null) {
      comment = anomaly.getFeedback().getComment();
      status = anomaly.getFeedback().getFeedbackType();
    }

    Map<String, String> externalUrls = ResourceUtils.getExternalURLs(anomaly, this.metricDAO);

    Multimap<String, String> attributes = ArrayListMultimap.create();
    attributes.put(ATTR_DATASET, anomaly.getCollection());
    attributes.put(ATTR_METRIC, anomaly.getMetric());
    attributes.put(ATTR_METRIC_ID, String.valueOf(metric.getId()));
    attributes.put(ATTR_FUNCTION, function.getFunctionName());
    attributes.put(ATTR_FUNCTION_ID, String.valueOf(function.getId()));
    attributes.put(ATTR_CURRENT, String.valueOf(anomaly.getAvgCurrentVal()));
    attributes.put(ATTR_BASELINE, String.valueOf(anomaly.getAvgBaselineVal()));
    attributes.put(ATTR_STATUS, status.toString());
    attributes.put(ATTR_COMMENT, comment);
    // attributes.put(ATTR_ISSUE_TYPE, null); // TODO
    attributes.putAll(ATTR_DIMENSIONS, anomaly.getDimensions().keySet());
    attributes.putAll(ATTR_EXTERNAL_URLS, externalUrls.keySet());
    attributes.put(ATTR_SCORE, String.valueOf(anomaly.getScore()));
    attributes.put(ATTR_WEIGHT, String.valueOf(anomaly.getWeight()));

    // external urls as attributes
    for (Map.Entry<String, String> entry : externalUrls.entrySet()) {
      attributes.put(entry.getKey(), entry.getValue());
    }

    // dimensions as attributes
    List<String> dimensionStrings = new ArrayList<>();
    for (Map.Entry<String, String> entry : anomaly.getDimensions().entrySet()) {
      dimensionStrings.add(entry.getValue());
      attributes.put(entry.getKey(), entry.getValue());
    }

    String dimensionString = "";
    if (!dimensionStrings.isEmpty()) {
      dimensionString = String.format(" (%s)", StringUtils.join(dimensionStrings, ", "));
    }

    String label = String.format("%s%s", function.getFunctionName(), dimensionString);
    String link = String.format("#/rootcause?anomalyId=%d", anomaly.getId());

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, anomaly.getStartTime(), anomaly.getEndTime(), null);
    out.setAttributes(attributes);

    return out;
  }

  private MetricConfigDTO getMetricFromFunction(AnomalyFunctionDTO function) {
    // NOTE this should be >= 0 but is a limitation of null field in the DB being converted to 0 by default
    if (function.getMetricId() > 0) {
      MetricConfigDTO metric = this.metricDAO.findById(function.getMetricId());
      if (metric == null) {
        throw new IllegalArgumentException(String.format("Could not resolve metric id %d", function.getMetricId()));
      }
      return metric;

    } else {
      MetricConfigDTO metric = this.metricDAO.findByMetricAndDataset(function.getMetric(), function.getCollection());
      if (metric == null) {
        throw new IllegalArgumentException(String.format("Could not resolve metric '%s' in dataset '%s'", function.getMetric(), function.getCollection()));
      }
      return metric;

    }
  }
}
