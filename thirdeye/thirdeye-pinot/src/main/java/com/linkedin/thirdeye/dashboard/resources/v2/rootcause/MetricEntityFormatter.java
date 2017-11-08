package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;


public class MetricEntityFormatter extends RootCauseEntityFormatter {
  public static final String TYPE_METRIC = "metric";

  public static final String ATTR_DATASET = "dataset";

  private final MetricConfigManager metricDAO;

  public MetricEntityFormatter(MetricConfigManager metricDAO) {
    this.metricDAO = metricDAO;
  }

  public MetricEntityFormatter() {
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
  }

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof MetricEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    MetricEntity e = (MetricEntity) entity;

    MetricConfigDTO dto = this.metricDAO.findById(e.getId());

    Multimap<String, String> attributes = ArrayListMultimap.create();
    attributes.put(ATTR_DATASET, dto.getDataset());

    String label = String.format("%s::%s", dto.getDataset(), dto.getName());

    RootCauseEntity out = makeRootCauseEntity(entity, TYPE_METRIC, label, null);
    out.setAttributes(attributes);

    return out;
  }
}
