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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of a pipeline for identifying relevant metric dimensions by performing
 * contribution analysis. The pipeline first fetches the TimeRange and Baseline entities and
 * MetricEntities in the search context. It then maps metric URNs to ThirdEye's internal database
 * and performs contribution analysis using a {@code MetricDimensionScorer).
 *
 * @see MetricDimensionScorer
 */
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
    Set<Entity> contextEntities = EntityUtils.filterContext(context, EntityType.METRIC);

    TimeRangeEntity current = EntityUtils.getContextTimeRange(context);
    if(current == null) {
      LOG.warn("Pipeline '{}' requires TimeRangeEntity. Skipping.", this.getName());
      return new PipelineResult(Collections.<Entity>emptyList());
    }

    BaselineEntity baseline = EntityUtils.getContextBaseline(context);
    if(baseline == null) {
      LOG.warn("Pipeline '{}' requires BaselineEntity. Skipping.", this.getName());
      return new PipelineResult(Collections.<Entity>emptyList());
    }

    Map<String, Entity> contextEntityMap = EntityUtils.mapEntityURNs(contextEntities);

    Map<String, MetricConfigDTO> metrics = new HashMap<>();
    Map<String, DatasetConfigDTO> datasets = new HashMap<>();
    for(Entity e : contextEntities) {
      String m = EntityUtils.getMetricName(e.getUrn());
      String d = EntityUtils.getMetricDataset(e.getUrn());

      MetricConfigDTO mdto = metricDAO.findByMetricAndDataset(m, d);
      DatasetConfigDTO ddto = datasetDAO.findByDataset(d);

      if(mdto == null) {
        LOG.warn("Could not resolve metric '{}'. Skipping.", m);
        continue;
      }

      if(ddto == null) {
        LOG.warn("Could not resolve dataset '{}'. Skipping metric '{}'", d, m);
        continue;
      }

      metrics.put(e.getUrn(), mdto);
      datasets.put(e.getUrn(), ddto);
    }

    LOG.info("Found {} metrics for dimension analysis", metrics.size());

    List<MetricDimensionEntity> entities = new ArrayList<>();
    for(Map.Entry<String, MetricConfigDTO> entry : metrics.entrySet()) {
      List<MetricDimensionEntity> local = new ArrayList<>();
      MetricConfigDTO mdto = entry.getValue();
      DatasetConfigDTO ddto = datasets.get(entry.getKey());

      for(String dim : ddto.getDimensions()) {
        double metricScore = contextEntityMap.get(entry.getKey()).getScore();
        local.add(MetricDimensionEntity.fromDTO(metricScore, mdto, ddto, dim));
      }

      try {
        entities.addAll(scorer.score(local, current, baseline));
      } catch (Exception e) {
        // TODO external exception handling?
        LOG.warn("Could not score entity '{}'", entry.getKey());
        e.printStackTrace();
        continue;
      }
    }

    return new PipelineResult(entities);
  }
}
