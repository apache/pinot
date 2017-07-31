package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pipeline for identifying relevant metrics based on dataset
 * association. The pipeline first fetches metric entities from the context and then
 * searches Thirdeye's internal database for metrics contained in the same datasets as
 * any metric entities in the search context. All found metrics are scored equally.
 */
public class MetricDatasetPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricDatasetPipeline.class);

  final MetricConfigManager metricDAO;
  final DatasetConfigManager datasetDAO;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   */
  public MetricDatasetPipeline(String outputName, Set<String> inputNames, MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO) {
    super(outputName, inputNames);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (none)
   */
  public MetricDatasetPipeline(String outputName, Set<String> inputNames, Map<String, Object> ignore) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = context.filter(MetricEntity.class);

    Set<String> datasets = new HashSet<>();
    Map<String, Double> datasetScores = new HashMap<>();
    Multimap<String, MetricEntity> related = ArrayListMultimap.create();
    for(MetricEntity me : metrics) {
      MetricConfigDTO metricDTO = this.metricDAO.findById(me.getId());

      String d = metricDTO.getDataset();
      datasets.add(d);

      double metricScore = me.getScore();
      if(!datasetScores.containsKey(d))
        datasetScores.put(d, 0.0d);
      datasetScores.put(d, datasetScores.get(d) + metricScore);

      related.put(d, me);
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

      dtos = removeExisting(dtos, metrics);

      for(MetricConfigDTO dto : dtos) {
        entities.add(MetricEntity.fromMetric(datasetScore, related.get(d), dto.getId()));
      }
    }

    return new PipelineResult(context, entities);
  }

  static Collection<MetricConfigDTO> removeExisting(Iterable<MetricConfigDTO> dtos, Iterable<MetricEntity> existing) {
    Collection<MetricConfigDTO> out = new ArrayList<>();
    for(MetricConfigDTO dto : dtos) {
      if(!findExisting(dto, existing))
        out.add(dto);
    }
    return out;
  }

  static boolean findExisting(MetricConfigDTO dto, Iterable<MetricEntity> existing) {
    for(MetricEntity me : existing) {
      if(me.getId() == dto.getId()) {
        return true;
      }
    }
    return false;
  }
}
