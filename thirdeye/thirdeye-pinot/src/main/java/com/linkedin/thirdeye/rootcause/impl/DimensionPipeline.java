package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of a pipeline for identifying relevant dimensions by performing
 * contribution analysis. The pipeline first fetches the Current and Baseline entities and
 * MetricEntities in the search context. It then maps the metrics to ThirdEye's internal database
 * and performs contribution analysis using a {@code DimensionScorer).
 *
 * @see DimensionScorer
 */
public class DimensionPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionPipeline.class);

  static final long TIMEOUT = 120000;

  static final String KEY = "key";
  static final String DIMENSION = DimensionScorer.DIMENSION;
  static final String VALUE = DimensionScorer.VALUE;
  static final String COST = DimensionScorer.COST;

  static final String PROP_PARALLELISM = "parallelism";

  final MetricConfigManager metricDAO;
  final DatasetConfigManager datasetDAO;
  final DimensionScorer scorer;
  final ExecutorService executor;

  public DimensionPipeline(String name, Set<String> inputs, MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO, DimensionScorer scorer, ExecutorService executor) {
    super(name, inputs);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.scorer = scorer;
    this.executor = executor;
  }

  public DimensionPipeline(String name, Set<String> inputs, Map<String, String> properties) {
    super(name, inputs);

    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.scorer = new DimensionScorer(ThirdEyeCacheRegistry.getInstance().getQueryCache());

    int parallelism = Integer.parseInt(properties.get(PROP_PARALLELISM));
    this.executor = Executors.newFixedThreadPool(parallelism);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metricsEntities = EntityUtils.filterContext(context, MetricEntity.class);

    final TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    final TimeRangeEntity baseline = TimeRangeEntity.getContextBaseline(context);

    DataFrame dfScore = new DataFrame();
    dfScore.addSeries(DIMENSION, StringSeries.empty());
    dfScore.addSeries(VALUE, StringSeries.empty());
    dfScore.addSeries(COST, DoubleSeries.empty());

    Map<String, Future<DataFrame>> scores = new HashMap<>();
    for(MetricEntity e : metricsEntities) {
      final MetricConfigDTO mdto = metricDAO.findByMetricAndDataset(e.getMetric(), e.getDataset());
      final DatasetConfigDTO ddto = datasetDAO.findByDataset(e.getDataset());

      if(mdto == null) {
        LOG.warn("Could not resolve metric '{}'. Skipping.", e.getMetric());
        continue;
      }

      if(ddto == null) {
        LOG.warn("Could not resolve dataset '{}'. Skipping metric '{}'", e.getDataset(), e.getMetric());
        continue;
      }

      // Create asynchronous scoring task
      final MetricEntity entity = e;
      Callable<DataFrame> scoreTask = new Callable<DataFrame>() {
        @Override
        public DataFrame call() throws Exception {
          LOG.info("Scoring metric '{}' in dataset '{}' with weight {}", entity.getMetric(), entity.getDataset(), entity.getScore());
          DataFrame dfMetric = scorer.score(ddto, mdto, current, baseline);

          // modify cost by metric score
          final double metricScore = entity.getScore();
          dfMetric.mapInPlace(new Series.DoubleFunction() {
            @Override
            public double apply(double... values) {
              return values[0] * metricScore;
            }
          }, COST);

          return dfMetric;
        }
      };

      Future<DataFrame> fScore = this.executor.submit(scoreTask);
      scores.put(e.getUrn(), fScore);
    }

    // Combine results
    for(Map.Entry<String, Future<DataFrame>> e : scores.entrySet()) {
      try {
        dfScore = dfScore.append(e.getValue().get(TIMEOUT, TimeUnit.MILLISECONDS));
      } catch (Exception ex) {
        LOG.warn("Exception while combining results for '{}'. Skipping.", e.getKey(), ex);
      }
    }

    // TODO use multi-column grouping when available
    // generate dimension keys
    dfScore.mapInPlace(new Series.StringFunction() {
      @Override
      public String apply(String... values) {
        return values[0] + ":" + values[1];
      }
    }, KEY, DIMENSION, VALUE);

    DataFrame.DataFrameGrouping grouping = dfScore.groupBy(KEY);
    DataFrame sumCost = grouping.aggregate(COST, DoubleSeries.SUM).fillNull(COST);
    DataFrame dimension = grouping.aggregate(DIMENSION, StringSeries.FIRST);
    DataFrame value = grouping.aggregate(VALUE, StringSeries.FIRST);

    // TODO cleanup
    // truncate results to most important dimensions
    DataFrame trunc = sumCost.sortedBy(COST);

    final double total = sumCost.getDoubles(COST).sum();
    final double truncTotal = trunc.getDoubles(COST).sum();
    LOG.info("Using {} out of {} scored dimensions, explaining {} of total differences", trunc.size(), sumCost.size(), truncTotal / total);

    DataFrame result = trunc.joinLeft(dimension).joinLeft(value);
    result.mapInPlace(new Series.DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] / total;
      }
    }, COST);

    return new PipelineResult(context, toEntities(result));
  }

  private static Set<DimensionEntity> toEntities(DataFrame df) {
    Set<DimensionEntity> entities = new HashSet<>();
    for(int i=0; i<df.size(); i++) {
      String dimension = df.getString(DIMENSION, i);
      String value = df.getString(VALUE, i);
      double score = df.getDouble(COST, i);
      entities.add(DimensionEntity.fromDimension(score, dimension, value));
    }
    return entities;
  }
}
