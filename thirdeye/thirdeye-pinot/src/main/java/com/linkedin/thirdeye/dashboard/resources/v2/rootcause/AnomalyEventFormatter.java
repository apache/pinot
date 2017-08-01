package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.impl.AnomalyEventEntity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;


public class AnomalyEventFormatter extends RootCauseEventEntityFormatter {
  private final static MetricEntityFormatter METRIC_FORMATTER = new MetricEntityFormatter();

  private final MetricConfigManager metricDAO;

  public AnomalyEventFormatter(MetricConfigManager metricDAO) {
    this.metricDAO = metricDAO;
  }

  public AnomalyEventFormatter() {
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
  }

  @Override
  public boolean applies(EventEntity entity) {
    return entity instanceof AnomalyEventEntity;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    AnomalyEventEntity e = (AnomalyEventEntity) entity;

    MergedAnomalyResultDTO dto = e.getDto();
    String label = String.format("Anomaly %d (%s)", dto.getId(), dto.getFunction().getFunctionName());

    // TODO add filters/dimensions as related entities

    return makeRootCauseEventEntity(entity, label, null, dto.getStartTime(), dto.getEndTime(), null);
  }
}
