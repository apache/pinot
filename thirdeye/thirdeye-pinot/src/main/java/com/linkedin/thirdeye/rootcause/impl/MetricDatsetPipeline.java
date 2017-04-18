package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of a pipeline for identifying relevant metrics based on dataset
 * association. The pipeline first fetches metric entities from the context and then
 * searches Thirdeye's internal database for metrics contained in the same datasets as
 * any metric entities in the search context. All found metrics are scored equally.
 */
public class MetricDatsetPipeline implements Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricDatsetPipeline.class);

  final MetricConfigManager metricDAO;
  final DatasetConfigManager datasetDAO;

  public MetricDatsetPipeline(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    Set<Entity> metrics = EntityUtils.filterContext(context, EntityUtils.EntityType.METRIC);

    Set<String> datasets = new HashSet<>();
    for(Entity m : metrics) {
      datasets.add(EntityUtils.getMetricDataset(m.getUrn()));
    }

    Collection<Entity> entities = new ArrayList<>();
    for(String d : datasets) {
      DatasetConfigDTO dataset = datasetDAO.findByDataset(d);
      if(dataset == null) {
        LOG.warn("Could not find dataset '{}'", d);
        continue;
      }

      Collection<MetricConfigDTO> dtos = metricDAO.findByDataset(d);
      for(MetricConfigDTO dto : dtos) {
        entities.add(MetricEntity.fromDTO(1.0, dto, dataset));
      }
    }

    return new PipelineResult(entities);
  }
}
