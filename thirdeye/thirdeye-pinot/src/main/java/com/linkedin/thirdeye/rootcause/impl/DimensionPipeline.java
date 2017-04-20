package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
public class DimensionPipeline implements Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionPipeline.class);

  public static final int DEFAULT_CUTOFF = 20;

  static final String KEY = "key";
  static final String DIMENSION = DimensionScorer.DIMENSION;
  static final String VALUE = DimensionScorer.VALUE;
  static final String COST = DimensionScorer.COST;

  final MetricConfigManager metricDAO;
  final DatasetConfigManager datasetDAO;
  final DimensionScorer scorer;

  final int cutoffSize;

  public DimensionPipeline(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      DimensionScorer scorer) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.scorer = scorer;
    this.cutoffSize = DEFAULT_CUTOFF;
  }

  public DimensionPipeline(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      DimensionScorer scorer, int cutoffSize) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.scorer = scorer;
    this.cutoffSize = cutoffSize;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    Set<MetricEntity> metricsEntities = EntityUtils.filterContext(context, MetricEntity.class);

    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    TimeRangeEntity baseline = TimeRangeEntity.getContextBaseline(context);

    DataFrame dfScore = new DataFrame();
    dfScore.addSeries(DIMENSION, StringSeries.empty());
    dfScore.addSeries(VALUE, StringSeries.empty());
    dfScore.addSeries(COST, DoubleSeries.empty());

    for(MetricEntity e : metricsEntities) {
      MetricConfigDTO mdto = metricDAO.findByMetricAndDataset(e.getMetric(), e.getDataset());
      DatasetConfigDTO ddto = datasetDAO.findByDataset(e.getDataset());

      if(mdto == null) {
        LOG.warn("Could not resolve metric '{}'. Skipping.", e.getMetric());
        continue;
      }

      if(ddto == null) {
        LOG.warn("Could not resolve dataset '{}'. Skipping metric '{}'", e.getDataset(), e.getMetric());
        continue;
      }

      LOG.info("Scoring metric '{}' in dataset '{}' with weight {}", e.getMetric(), e.getDataset(), e.getScore());
      try {
        DataFrame dfMetric = this.scorer.score(ddto, mdto, current, baseline);

        // modify cost by metric weight
        final double metricWeight = e.getScore();
        dfMetric.mapInPlace(new Series.DoubleFunction() {
          @Override
          public double apply(double... values) {
            return values[0] * metricWeight;
          }
        }, COST);

        dfScore = dfScore.append(dfMetric);
      } catch (Exception ignore) {
        // left blank
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
    DataFrame cost = grouping.aggregate(COST, DoubleSeries.SUM);
    DataFrame dimension = grouping.aggregate(DIMENSION, StringSeries.FIRST);
    DataFrame value = grouping.aggregate(VALUE, StringSeries.FIRST);

    // truncate results to most important dimensions
    DataFrame trunc = cost.sortedBy(COST).reverse().slice(0, this.cutoffSize);

    double explained = trunc.getDoubles(COST).sum() / trunc.size();
    LOG.info("Using {} out of {} scored dimensions, explaining {} of total differences", trunc.size(), cost.size(), explained);

    DataFrame result = trunc.joinLeft(dimension).joinLeft(value);

    return new PipelineResult(toEntities(result));
  }

  private static List<DimensionEntity> toEntities(DataFrame df) {
    List<DimensionEntity> entities = new ArrayList<>();
    for(int i=0; i<df.size(); i++) {
      String dimension = df.getString(DIMENSION, i);
      String value = df.getString(VALUE, i);
      double score = df.getDouble(COST, i);
      entities.add(DimensionEntity.fromDimension(score, dimension, value));
    }
    return entities;
  }
}
