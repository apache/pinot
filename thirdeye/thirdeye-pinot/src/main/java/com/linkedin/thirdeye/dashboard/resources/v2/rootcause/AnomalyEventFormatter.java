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
  public static final String ATTR_FUNCTION = "function";
  public static final String ATTR_CURRENT = "current";
  public static final String ATTR_BASELINE = "baseline";
  public static final String ATTR_STATUS = "status";
  public static final String ATTR_ISSUE_TYPE = "issueType";
  public static final String ATTR_DIMENSIONS = "dimensions";
  public static final String ATTR_COMMENT = "comment";
  public static final String ATTR_EXTERNAL_URLS = "externalUrls";

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

    MergedAnomalyResultDTO dto = this.anomalyDAO.findById(e.getId(), false);
    AnomalyFunctionDTO func = dto.getFunction();

    String comment = "";
    AnomalyFeedbackType status = AnomalyFeedbackType.NO_FEEDBACK;
    if (dto.getFeedback() != null) {
      comment = dto.getFeedback().getComment();
      status = dto.getFeedback().getFeedbackType();
    }

    Map<String, String> externalUrls = ResourceUtils.getExternalURLs(dto, metricDAO);

    Multimap<String, String> attributes = ArrayListMultimap.create();
    attributes.put(ATTR_DATASET, dto.getCollection());
    attributes.put(ATTR_METRIC, dto.getMetric());
    attributes.put(ATTR_FUNCTION, func.getFunctionName());
    attributes.put(ATTR_CURRENT, String.valueOf(dto.getAvgCurrentVal()));
    attributes.put(ATTR_BASELINE, String.valueOf(dto.getAvgBaselineVal()));
    attributes.put(ATTR_STATUS, status.toString());
    attributes.put(ATTR_COMMENT, comment);
    // attributes.put(ATTR_ISSUE_TYPE, null); // TODO
    attributes.putAll(ATTR_DIMENSIONS, dto.getDimensions().keySet());
    attributes.putAll(ATTR_EXTERNAL_URLS, externalUrls.keySet());

    // external urls as attributes
    for (Map.Entry<String, String> entry : externalUrls.entrySet()) {
      attributes.put(entry.getKey(), entry.getValue());
    }

    // dimensions as attributes
    List<String> dimensionStrings = new ArrayList<>();
    for (Map.Entry<String, String> entry : dto.getDimensions().entrySet()) {
      dimensionStrings.add(entry.getValue());
      attributes.put(entry.getKey(), entry.getValue());
    }

    String dimensionString = "";
    if (!dimensionStrings.isEmpty()) {
      dimensionString = String.format(" (%s)", StringUtils.join(dimensionStrings, ", "));
    }

    String label = String.format("%s%s", func.getFunctionName(), dimensionString);
    String link = String.format("#/rootcause?anomalyId=%d", dto.getId());

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, dto.getStartTime(), dto.getEndTime(), null);
    out.setAttributes(attributes);

    return out;
  }
}
