package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pipeline for identifying relevant dimensions by performing
 * contribution analysis. The pipeline first fetches the Current and Baseline entities and
 * MetricEntities in the search context. It then maps the metrics to ThirdEye's internal database
 * and performs contribution analysis using a {@code DimensionScorer).
 *
 * @see DimensionScorer
 */
public class DimensionAnalysisPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionAnalysisPipeline.class);

  public static final String COL_CONTRIB = "contribution";
  public static final String COL_DELTA = "delta";
  public static final String COL_DIM_NAME = "dimension";
  public static final String COL_DIM_VALUE = "value";
  public static final String COL_SCORE = "score";

  public static final String PROP_PARALLELISM = "parallelism";
  public static final int PROP_PARALLELISM_DEFAULT = 1;

  public static final String PROP_K = "k";
  public static final int PROP_K_DEFAULT = -1;

  public static final long TIMEOUT = 120000;

  private final QueryCache cache;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final ExecutorService executor;
  private final int k;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param metricDAO metric config DAO
   * @param datasetDAO dataset config DAO
   * @param cache query cache for running contribution analysis
   * @param executor executor service for parallel task execution
   */
  public DimensionAnalysisPipeline(String outputName, Set<String> inputNames, MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO, QueryCache cache, ExecutorService executor, int k) {
    super(outputName, inputNames);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
    this.executor = executor;
    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K}, {@code PROP_PARALLELISM})
   */
  public DimensionAnalysisPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    this.executor = Executors.newFixedThreadPool(MapUtils.getInteger(properties, PROP_PARALLELISM, PROP_PARALLELISM_DEFAULT));
    this.k = MapUtils.getInteger(properties, PROP_K, PROP_K_DEFAULT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metricsEntities = context.filter(MetricEntity.class);

    final TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    final TimeRangeEntity baseline = TimeRangeEntity.getContextBaseline(context);

    Map<Dimension, Double> scores = new HashMap<>();
    for(MetricEntity me : metricsEntities) {
      try {
        final long windowSize = current.getEnd() - current.getStart();
        DataFrame dfScores = getDimensionScores(me.getId(), current.getStart(), baseline.getStart(), windowSize);

        for(int i=0; i<dfScores.size(); i++) {
          double score = dfScores.getDouble(COL_SCORE, i);
          Dimension d = new Dimension(dfScores.getString(COL_DIM_NAME, i), dfScores.getString(COL_DIM_VALUE, i));
          if(!scores.containsKey(d))
            scores.put(d, 0.0d);
          scores.put(d, scores.get(d) + score * me.getScore());
        }
      } catch (Exception e) {
        LOG.warn("Error calculating dimension scores for '{}'. Skipping.", me.getUrn(), e);
      }
    }

    Set<DimensionEntity> entities = new HashSet<>();
    for(Map.Entry<Dimension, Double> entry : scores.entrySet()) {
      entities.add(entry.getKey().toEntity(entry.getValue()));
    }

    return new PipelineResult(context, EntityUtils.topkNormalized(entities, this.k));
  }

  private DataFrame getContribution(long metricId, long start, long end, String dimension) throws Exception {
    String ref = String.format("%d-%s", metricId, dimension);
    DataFrameUtils.RequestContainer rc = DataFrameUtils.makeAggregateRequest(metricId, start, end, Arrays.asList(dimension), ref, this.metricDAO, this.datasetDAO);
    ThirdEyeResponse res = this.cache.getQueryResult(rc.getRequest());

    DataFrame raw = DataFrameUtils.evaluateResponse(res, rc);

    DataFrame out = new DataFrame();
    out.addSeries(dimension, raw.getStrings(dimension));
    out.addSeries(COL_CONTRIB, raw.getDoubles(DataFrameUtils.COL_VALUE).normalizeSum());
    out.setIndex(dimension);

    return out;
  }

  private DataFrame getContributionDelta(long metricId, long current, long baseline, long windowSize, String dimension) throws Exception {
    DataFrame curr = getContribution(metricId, current, current + windowSize, dimension);
    DataFrame base = getContribution(metricId, baseline, baseline + windowSize, dimension);

    DataFrame joined = curr.joinOuter(base)
        .fillNull(COL_CONTRIB + DataFrame.COLUMN_JOIN_LEFT)
        .fillNull(COL_CONTRIB + DataFrame.COLUMN_JOIN_RIGHT);

    DoubleSeries diff = joined.getDoubles(COL_CONTRIB + DataFrame.COLUMN_JOIN_LEFT)
        .subtract(joined.getDoubles(COL_CONTRIB + DataFrame.COLUMN_JOIN_RIGHT));

    DataFrame df = new DataFrame();
    df.addSeries(dimension, joined.getStrings(dimension));
    df.addSeries(COL_CONTRIB, joined.getDoubles(COL_CONTRIB + DataFrame.COLUMN_JOIN_LEFT));
    df.addSeries(COL_DELTA, diff);
    df.setIndex(dimension);

    return df;
  }

  private DataFrame packDimension(DataFrame dfDelta, String dimension) {
    DataFrame df = new DataFrame();
    df.addSeries(COL_CONTRIB, dfDelta.get(COL_CONTRIB));
    df.addSeries(COL_DELTA, dfDelta.get(COL_DELTA));
    df.addSeries(COL_DIM_NAME, StringSeries.fillValues(dfDelta.size(), dimension));
    df.addSeries(COL_DIM_VALUE, dfDelta.get(dimension));
    return df;
  }

  private Future<DataFrame> getContributionDeltaPackedAsync(final long metricId, final long current, final long baseline, final long windowSize, final String dimension) throws Exception {
    return this.executor.submit(new Callable<DataFrame>() {
      @Override
      public DataFrame call() throws Exception {
        return packDimension(getContributionDelta(metricId, current, baseline, windowSize, dimension), dimension);
      }
    });
  }

  private DataFrame getDimensionScores(long metricId, long current, long baseline, long windowSize) throws Exception {
    MetricConfigDTO metric = this.metricDAO.findById(metricId);
    if(metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id '%d'", metricId));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));
    }

    if(!dataset.isAdditive()) {
      LOG.warn("Contribution analysis on non-additive dataset");

      // TODO read additive from metric property when available
      //throw new IllegalArgumentException(String.format("Requires additive dataset, but '%s' isn't.", dataset.getDataset()));
    }

    Collection<Future<DataFrame>> futures = new ArrayList<>();
    for(String dimension : dataset.getDimensions()) {
      futures.add(getContributionDeltaPackedAsync(metricId, current, baseline, windowSize, dimension));
    }

    final long timeout = System.currentTimeMillis() + TIMEOUT;
    Collection<DataFrame> contributors = new ArrayList<>();
    for(Future<DataFrame> future : futures) {
      final long timeLeft = Math.max(timeout - System.currentTimeMillis(), 0);
      contributors.add(future.get(timeLeft, TimeUnit.MILLISECONDS));
    }

    DataFrame combined = DataFrame.builder(
        COL_DIM_NAME + ":STRING",
        COL_DIM_VALUE + ":STRING",
        COL_CONTRIB + ":DOUBLE",
        COL_DELTA + ":DOUBLE").build();

    combined = combined.append(contributors.toArray(new DataFrame[contributors.size()]));
    combined.addSeries(COL_SCORE, combined.getDoubles(COL_DELTA).abs());

    return combined.sortedBy(COL_SCORE).reverse();
  }

  private static class Dimension {
    final String name;
    final String value;

    public Dimension(String name, String value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    public DimensionEntity toEntity(double score) {
      return DimensionEntity.fromDimension(score, this.name, this.value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Dimension dimension = (Dimension) o;
      return Objects.equals(name, dimension.name) && Objects.equals(value, dimension.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }
  }

}
