package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

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
  @Override
  public boolean applies(EventEntity entity) {
    return entity instanceof AnomalyEventEntity;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    AnomalyEventEntity e = (AnomalyEventEntity) entity;

    MergedAnomalyResultDTO dto = e.getDto();
    AnomalyFunctionDTO func = dto.getFunction();

    List<String> dimensions = new ArrayList<>();
    for (Map.Entry<String, String> entry : dto.getDimensions().entrySet()) {
      dimensions.add(entry.getKey() + ":" + entry.getValue().toUpperCase());
    }

    String label = String.format("%s (%s)", func.getFunctionName(), StringUtils.join(dimensions, ", "));
    String link = String.format("thirdeye#investigate?anomalyId=%d", dto.getId());

    return makeRootCauseEventEntity(entity, label, link, dto.getStartTime(), dto.getEndTime(), null);
  }
}
