package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.impl.AnomalyEventEntity;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import com.linkedin.thirdeye.rootcause.impl.HolidayEventEntity;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.List;
import java.util.Map;


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
    String label = dto.getFunction().getFunctionName();
    String link =  "javascript:void(0);";

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, dto.getStartTime(), dto.getEndTime(), "");

    // TODO use metric id when available
    String metric = dto.getMetric();
    String dataset = dto.getCollection();
    MetricConfigDTO metricDTO = this.metricDAO.findByMetricAndDataset(metric, dataset);

    MetricEntity me = MetricEntity.fromMetric(1.0, metricDTO.getId());
    out.addRelatedEntity(METRIC_FORMATTER.format(me));

    return out;
  }
}
