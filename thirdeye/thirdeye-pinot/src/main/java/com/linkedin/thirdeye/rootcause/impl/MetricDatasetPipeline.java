package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of a pipeline for identifying relevant metrics based on dataset
 * association. The pipeline first fetches metric entities from the context and then
 * searches Thirdeye's internal database for metrics contained in the same datasets as
 * any metric entities in the search context. All found metrics are scored equally.
 */
public class MetricDatasetPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricDatasetPipeline.class);

  final MetricConfigManager metricDAO;
  final DatasetConfigManager datasetDAO;

  public MetricDatasetPipeline(String name, Set<String> inputs, MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO) {
    super(name, inputs);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = EntityUtils.filterContext(context, MetricEntity.class);

    Set<String> datasets = new HashSet<>();
    Map<String, Double> datasetScores = new HashMap<>();
    for(MetricEntity m : metrics) {
      String d = m.getDataset();
      datasets.add(d);

      double metricScore = m.getScore();
      if(!datasetScores.containsKey(d))
        datasetScores.put(d, 0.0d);
      datasetScores.put(d, datasetScores.get(d) + metricScore);
    }

    Set<Entity> entities = new HashSet<>();
    for(String d : datasets) {
      DatasetConfigDTO dataset = datasetDAO.findByDataset(d);
      if(dataset == null) {
        LOG.warn("Could not find dataset '{}'", d);
        continue;
      }

      double datasetScore = datasetScores.get(d);
      Collection<MetricConfigDTO> dtos = metricDAO.findByDataset(d);
      for(MetricConfigDTO dto : dtos) {
        double score = datasetScore / dtos.size();
        entities.add(MetricEntity.fromMetric(score, d, dto.getName()));
      }
    }

    return new PipelineResult(context, entities);
  }
}
