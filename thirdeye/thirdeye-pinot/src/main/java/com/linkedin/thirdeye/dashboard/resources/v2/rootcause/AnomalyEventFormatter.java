package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
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

  private final MergedAnomalyResultManager anomalyDAO;

  public AnomalyEventFormatter() {
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
  }

  public AnomalyEventFormatter(MergedAnomalyResultManager anomalyDAO) {
    this.anomalyDAO = anomalyDAO;
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

    AnomalyFeedbackType feedback = AnomalyFeedbackType.NO_FEEDBACK;
    if (dto.getFeedback() != null) {
      feedback = dto.getFeedback().getFeedbackType();
    }

    Multimap<String, String> attributes = ArrayListMultimap.create();
    attributes.put(ATTR_DATASET, dto.getCollection());
    attributes.put(ATTR_METRIC, dto.getMetric());
    attributes.put(ATTR_FUNCTION, func.getFunctionName());
    attributes.put(ATTR_CURRENT, String.valueOf(dto.getAvgCurrentVal()));
    attributes.put(ATTR_BASELINE, String.valueOf(dto.getAvgBaselineVal()));
    attributes.put(ATTR_STATUS, feedback.toString());
    // attributes.put(ATTR_ISSUE_TYPE, null); // TODO
    attributes.putAll(ATTR_DIMENSIONS, dto.getDimensions().keySet());

    List<String> dimensionStrings = new ArrayList<>();
    for (Map.Entry<String, String> entry : dto.getDimensions().entrySet()) {
      dimensionStrings.add(entry.getKey() + "=" + entry.getValue());
      attributes.put(entry.getKey(), entry.getValue());
    }

    String label = String.format("%s (%s)", func.getFunctionName(), StringUtils.join(dimensionStrings, ", "));
    String link = String.format("thirdeye#investigate?anomalyId=%d", dto.getId());

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, dto.getStartTime(), dto.getEndTime(), null);
    out.setAttributes(attributes);

    return out;
  }
}
