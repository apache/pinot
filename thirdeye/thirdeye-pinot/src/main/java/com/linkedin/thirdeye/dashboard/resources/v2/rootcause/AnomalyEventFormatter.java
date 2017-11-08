package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.impl.AnomalyEventEntity;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;


public class AnomalyEventFormatter extends RootCauseEventEntityFormatter {
  public static final String ATTR_DATASET = "dataset";
  public static final String ATTR_METRIC = "dataset";

  @Override
  public boolean applies(EventEntity entity) {
    return entity instanceof AnomalyEventEntity;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    AnomalyEventEntity e = (AnomalyEventEntity) entity;

    MergedAnomalyResultDTO dto = e.getDto();
    AnomalyFunctionDTO func = dto.getFunction();

    Multimap<String, String> attributes = ArrayListMultimap.create();
    attributes.put(ATTR_DATASET, dto.getCollection());
    attributes.put(ATTR_METRIC, dto.getMetric());

    List<String> dimensions = new ArrayList<>();
    for (Map.Entry<String, String> entry : dto.getDimensions().entrySet()) {
      dimensions.add(entry.getKey() + ":" + entry.getValue());
      attributes.put(entry.getKey(), entry.getValue());
    }

    String label = String.format("%s (%s)", func.getFunctionName(), StringUtils.join(dimensions, ", "));
    String link = String.format("thirdeye#investigate?anomalyId=%d", dto.getId());

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, dto.getStartTime(), dto.getEndTime(), null);
    out.setAttributes(attributes);

    return out;
  }
}
