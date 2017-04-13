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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricDimensionPipeline implements Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricDimensionPipeline.class);

  final MetricConfigManager metricDAO;
  final DatasetConfigManager datasetDAO;
  final MetricDimensionScorer scorer;

  public MetricDimensionPipeline(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      MetricDimensionScorer scorer) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.scorer = scorer;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    Set<String> metrics = new HashSet<>();
    Set<String> datasets = new HashSet<>();
    for(Entity e : context.getSearchContext().getEntities()) {
      if(URNUtils.isMetricURN(e.getUrn())) {
        metrics.add(URNUtils.getMetricName(e.getUrn()));
        datasets.add(URNUtils.getMetricDataset(e.getUrn()));
      }
    }

    // TODO confirm usage of MetricDTO in ThirdEye
    // is it metric-dimension already?

    LOG.info("Found {} metrics across {} datasets for dimension analysis", metrics.size(), datasets.size());

    Map<String, DatasetConfigDTO> datasetMap = new HashMap<>();
    for(String d : datasets) {
      DatasetConfigDTO dto = datasetDAO.findByDataset(d);
      datasetMap.put(dto.getDataset(), dto);
    }

    Map<MetricDimensionEntity, Double> scores = new HashMap<>();
    for(String m : metrics) {
      List<MetricConfigDTO> dtos = metricDAO.findByMetricName(m);
      for(MetricConfigDTO mdto : dtos) {
        DatasetConfigDTO ddto = datasetMap.get(mdto.getDataset());

        if(ddto == null) {
          LOG.warn("Skipping metric '{}'. Could not resolve associated dataset '{}'", m, mdto.getName());
          continue;
        }

        List<MetricDimensionEntity> entities = new ArrayList<>();
        for(String dim : ddto.getDimensions()) {
          entities.add(MetricDimensionEntity.fromDTO(mdto, ddto, dim));
        }

        try {
          scores.putAll(scorer.score(entities, context.getSearchContext()));
        } catch (Exception e) {
          // TODO internal exception handling?
          throw new RuntimeException(e);
        }
      }
    }

    return new PipelineResult(scores);
  }
}
