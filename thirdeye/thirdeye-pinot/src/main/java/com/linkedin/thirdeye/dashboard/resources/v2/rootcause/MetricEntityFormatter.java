package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;


public class MetricEntityFormatter extends RootCauseEntityFormatter {
  public static final String TYPE_METRIC = "metric";

  public static final String ATTR_DATASET = "dataset";
  public static final String ATTR_INVERSE = "inverse";
  public static final String ATTR_DERIVED = "derived";
  public static final String ATTR_ADDITIVE = "additive";

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;

  public MetricEntityFormatter(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
  }

  public MetricEntityFormatter() {
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
  }

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof MetricEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    MetricEntity e = (MetricEntity) entity;

    MetricConfigDTO metric = this.metricDAO.findById(e.getId());
    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());

    Multimap<String, String> attributes = ArrayListMultimap.create();
    attributes.put(ATTR_DATASET, metric.getDataset());
    attributes.put(ATTR_INVERSE, String.valueOf(metric.isInverseMetric()));
    attributes.put(ATTR_DERIVED, String.valueOf(metric.isDerived()));
    attributes.put(ATTR_ADDITIVE, String.valueOf(dataset.isAdditive()));

    String label = String.format("%s::%s", metric.getDataset(), metric.getName());

    RootCauseEntity out = makeRootCauseEntity(entity, TYPE_METRIC, label, null);
    out.setAttributes(attributes);

    return out;
  }
}
