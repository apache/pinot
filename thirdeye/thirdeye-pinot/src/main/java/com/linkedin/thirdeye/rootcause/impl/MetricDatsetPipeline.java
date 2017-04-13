package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class MetricDatsetPipeline implements Pipeline {
  final MetricConfigManager metricDAO;

  public MetricDatsetPipeline(MetricConfigManager metricDAO) {
    this.metricDAO = metricDAO;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    Set<Entity> metrics = URNUtils.filterContext(context, URNUtils.EntityType.METRIC);

    Set<String> datasets = new HashSet<>();
    for(Entity m : metrics) {
      datasets.add(URNUtils.getMetricDataset(m.getUrn()));
    }

    Map<MetricEntity, Double> scores = new HashMap<>();
    for(String d : datasets) {
      Collection<MetricConfigDTO> dtos = metricDAO.findByDataset(d);
      for(MetricConfigDTO dto : dtos) {
        scores.put(MetricEntity.fromDTO(dto), 1.0);
      }
    }

    return new PipelineResult(scores);
  }
}
