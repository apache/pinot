package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;


public class MetricEntityFormatter extends RootCauseEntityFormatter {
  private final MetricConfigManager metricDAO;

  public MetricEntityFormatter(MetricConfigManager metricDAO) {
    this.metricDAO = metricDAO;
  }

  public MetricEntityFormatter() {
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
  }

  @Override
  public boolean applies(Entity entity) {
    return MetricEntity.TYPE.isType(entity.getUrn());
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    MetricEntity e = MetricEntity.fromURN(entity.getUrn(), entity.getScore());

    MetricConfigDTO metricDTO = this.metricDAO.findById(e.getId());

    String label = String.format("unknown (id=%d)", e.getId());
    if(metricDTO != null)
        label = String.format("%s/%s", metricDTO.getDataset(), metricDTO.getName());

    String link = String.format("javascript:alert('%s');", e.getUrn());

    return makeRootCauseEntity(entity, "Metric", label, link);
  }
}
