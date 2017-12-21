package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MetricMappingPipeline maps metrics to related metrics via dataset and entity mappings.
 * Also translates dimension filters between related metric entities if possible.<br />
 *
 * <br/><b>NOTE:</b> traverses a maximum of one hop for related metrics and related datasets.
 * Performs 2nd degree search for metric > dataset > related dataset > related metric
 */
public class MetricMappingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricMappingPipeline.class);

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EntityToEntityMappingManager mappingDAO;

  /**
   * Constructor for dependency injection
   */
  public MetricMappingPipeline(String outputName, Set<String> inputNames, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, EntityToEntityMappingManager mappingDAO) {
    super(outputName, inputNames);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.mappingDAO = mappingDAO;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (ignored)
   */
  public MetricMappingPipeline(String outputName, Set<String> inputNames, Map<String, Object> ignore) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.mappingDAO = DAORegistry.getInstance().getEntityToEntityMappingDAO();
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> input = context.filter(MetricEntity.class);
    Set<MetricEntity> output = new MaxScoreSet<>();

    for (MetricEntity me : input) {
      Multimap<String, String> filters = me.getFilters();

      MetricConfigDTO metric = this.metricDAO.findById(me.getId());
      if (metric == null) {
        LOG.warn("Could not resolve metric id {}. Skipping.", me.getId());
        continue;
      }

      DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
      if (dataset == null) {
        LOG.warn("Could not resolve metric id {} dataset '{}'. Skipping.", me.getId(), metric.getDataset());
        continue;
      }

      DatasetEntity de = DatasetEntity.fromName(me.getScore(), metric.getDataset());

      Set<MetricEntity> metrics = new MaxScoreSet<>();
      Set<DatasetEntity> datasets = new MaxScoreSet<>();

      // metric
      // NOTE: native metric added via native dataset

      // dataset
      datasets.add(de);

      // from metric
      List<EntityToEntityMappingDTO> fromMetric = this.mappingDAO.findByFromURN(me.withoutFilters().getUrn());
      for (EntityToEntityMappingDTO mapping : fromMetric) {
        String urn = mapping.getToURN();

        // metric-related metrics
        if (MetricEntity.TYPE.isType(urn)) {
          MetricEntity m = MetricEntity.fromURN(urn, me.getScore() * mapping.getScore());
          metrics.add(m.withFilters(pruneFilters(filters, m.getId())));
        }

        // metric-related datasets
        if (DatasetEntity.TYPE.isType(urn)) {
          datasets.add(DatasetEntity.fromURN(urn, me.getScore() * mapping.getScore()));
        }
      }

      // from dataset
      List<EntityToEntityMappingDTO> fromDataset = this.mappingDAO.findByFromURN(de.getUrn());
      for (EntityToEntityMappingDTO mapping : fromDataset) {
        String urn = mapping.getToURN();

        // NOTE: dataset-native metrics explored with datasets below

        // dataset-related datasets
        if (DatasetEntity.TYPE.isType(urn)) {
          DatasetEntity relatedDataset = DatasetEntity.fromURN(urn, de.getScore() * mapping.getScore());
          datasets.add(relatedDataset);
        }
      }

      // from related datasets (and dataset)
      for (DatasetEntity relatedDataset : datasets) {
        List<MetricConfigDTO> nativeMetrics = this.metricDAO.findByDataset(relatedDataset.getName());

        // related-dataset-native metrics
        for (MetricConfigDTO nativeMetric : nativeMetrics) {
          MetricEntity m = MetricEntity.fromMetric(relatedDataset.getScore(), nativeMetric.getId());
          metrics.add(m.withFilters(pruneFilters(filters, m.getId())));
        }

        // related-dataset-related metrics
        // NOTE: potentially expensive 2nd degree search
        List<EntityToEntityMappingDTO> relatedMetrics = this.mappingDAO.findByFromURN(relatedDataset.getUrn());
        for (EntityToEntityMappingDTO relatedMetric : relatedMetrics) {
          if (MetricEntity.TYPE.isType(relatedMetric.getToURN())) {
            MetricEntity m = MetricEntity.fromURN(relatedMetric.getToURN(), relatedDataset.getScore() * relatedMetric.getScore());
            metrics.add(m.withFilters(pruneFilters(filters, m.getId())));
          }
        }
      }

      output.addAll(metrics);
    }

    return new PipelineResult(context, output);
  }

  /**
   * Prunes filter set to only allow dimensions that are available in a metrics own dataset
   *
   * @param metricId metric id
   * @return pruned filter multimap
   */
  private Multimap<String, String> pruneFilters(Multimap<String, String> filters, long metricId) {
    MetricConfigDTO metric = this.metricDAO.findById(metricId);
    if (metric == null) {
      LOG.warn("Could not resolve metric id {} while pruning filters", metricId);
      return ArrayListMultimap.create();
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      LOG.warn("Could not resolve dataset '{}' for metric id {} while pruning filters", metric.getDataset(), metricId);
      return ArrayListMultimap.create();
    }

    Multimap<String, String> output = ArrayListMultimap.create();
    Set<String> validKeys = new HashSet<>(dataset.getDimensions());
    for (Map.Entry<String, String> entry : filters.entries()) {
      if (validKeys.contains(entry.getKey())) {
        output.put(entry.getKey(), entry.getValue());
      }
    }
    return output;
  }
}
