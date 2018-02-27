package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.dataframe.util.RequestContainer;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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
 * and performs contribution analysis.
 *
 * <br/><b>NOTE:</b> This is the successor to DimensionAnalysisPipeline. It relies on MetricEntities
 * with filters in the URN tail instead of DimensionEntities.
 *
 * @see DimensionAnalysisPipeline
 */
public class MetricBreakdownPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricBreakdownPipeline.class);

  public static final String COL_CONTRIB = "contribution";
  public static final String COL_DELTA = "delta";
  public static final String COL_DIM_NAME = "dimension";
  public static final String COL_DIM_VALUE = "value";
  public static final String COL_SCORE = "score";

  public static final String PROP_PARALLELISM = "parallelism";
  public static final int PROP_PARALLELISM_DEFAULT = 1;

  public static final String PROP_K = "k";
  public static final int PROP_K_DEFAULT = -1;

  public static final String PROP_INCLUDE_DIMENSIONS = "includeDimensions";
  public static final String PROP_EXCLUDE_DIMENSIONS = "excludeDimensions";

  public static final long TIMEOUT = 120000;

  private final QueryCache cache;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final ExecutorService executor;
  private final Set<String> includeDimensions;
  private final Set<String> excludeDimensions;
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
   * @param includeDimensions dimensions to break down on (all if empty)
   * @param excludeDimensions dimensions not to break down on
   * @param k number of top-ranking elements to emit
   */
  public MetricBreakdownPipeline(String outputName, Set<String> inputNames, MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO, QueryCache cache, ExecutorService executor, Set<String> includeDimensions, Set<String> excludeDimensions, int k) {
    super(outputName, inputNames);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
    this.executor = executor;
    this.includeDimensions = includeDimensions;
    this.excludeDimensions = excludeDimensions;
    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K}, {@code PROP_PARALLELISM})
   */
  public MetricBreakdownPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    this.executor = Executors.newFixedThreadPool(MapUtils.getInteger(properties, PROP_PARALLELISM, PROP_PARALLELISM_DEFAULT));
    this.k = MapUtils.getInteger(properties, PROP_K, PROP_K_DEFAULT);

    if (properties.containsKey(PROP_INCLUDE_DIMENSIONS)) {
      this.includeDimensions = new HashSet<>((Collection<String>) properties.get(PROP_INCLUDE_DIMENSIONS));
    } else {
      this.includeDimensions = new HashSet<>();
    }

    if (properties.containsKey(PROP_EXCLUDE_DIMENSIONS)) {
      this.excludeDimensions = new HashSet<>((Collection<String>) properties.get(PROP_EXCLUDE_DIMENSIONS));
    } else {
      this.excludeDimensions = new HashSet<>();
    }
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metricsEntities = context.filter(MetricEntity.class);

    final TimeRangeEntity anomaly = TimeRangeEntity.getTimeRangeAnomaly(context);
    final TimeRangeEntity baseline = TimeRangeEntity.getTimeRangeBaseline(context);

    Set<MetricEntity> output = new MaxScoreSet<>();

    for (MetricEntity me : metricsEntities) {
      try {
        final MetricSlice sliceCurrent = MetricSlice.from(me.getId(), anomaly.getStart(), anomaly.getEnd(), me.getFilters());
        final MetricSlice sliceBaseline = MetricSlice.from(me.getId(), baseline.getStart(), baseline.getEnd(), me.getFilters());

        DataFrame dfScores = getDimensionScores(sliceCurrent, sliceBaseline);

        for (int i=0; i<dfScores.size(); i++) {
          String name = dfScores.getString(COL_DIM_NAME, i);
          String value = dfScores.getString(COL_DIM_VALUE, i);
          double score = dfScores.getDouble(COL_SCORE, i);

          if (score <= 0)
            continue;

          Multimap<String, String> newFilters = TreeMultimap.create(me.getFilters());
          newFilters.put(name, value);

          output.add(me.withScore(me.getScore() * score).withFilters(newFilters));
        }
      } catch (Exception e) {
        LOG.warn("Error calculating dimension scores for '{}'. Skipping.", me.getUrn(), e);
      }
    }

    return new PipelineResult(context, EntityUtils.topkNormalized(output, this.k));
  }

  private DataFrame getContribution(MetricSlice slice, String dimension) throws Exception {
    String ref = String.format("%d-%s", slice.getMetricId(), dimension);
    RequestContainer rc = DataFrameUtils.makeAggregateRequest(slice, Collections.singletonList(dimension), ref, this.metricDAO, this.datasetDAO);
    ThirdEyeResponse res = this.cache.getQueryResult(rc.getRequest());

    DataFrame raw = DataFrameUtils.evaluateResponse(res, rc);

    DataFrame out = new DataFrame();
    out.addSeries(dimension, raw.getStrings(dimension));
    out.addSeries(COL_CONTRIB, raw.getDoubles(DataFrameUtils.COL_VALUE).normalizeSum());
    out.setIndex(dimension);

    return out;
  }

  private DataFrame getContributionDelta(MetricSlice current, MetricSlice baseline, String dimension) throws Exception {
    DataFrame curr = getContribution(current, dimension);
    DataFrame base = getContribution(baseline, dimension);

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

  private Future<DataFrame> getContributionDeltaPackedAsync(final MetricSlice current, final MetricSlice baseline, final String dimension) throws Exception {
    return this.executor.submit(new Callable<DataFrame>() {
      @Override
      public DataFrame call() throws Exception {
        return packDimension(getContributionDelta(current, baseline, dimension), dimension);
      }
    });
  }

  private DataFrame getDimensionScores(MetricSlice current, MetricSlice baseline) throws Exception {
    if (current.getMetricId() != baseline.getMetricId()) {
      throw new IllegalArgumentException("current and baseline must reference the same metric id");
    }

    MetricConfigDTO metric = this.metricDAO.findById(current.getMetricId());
    if(metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id '%d'", current.getMetricId()));
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
      if (this.excludeDimensions.contains(dimension)) {
        continue;
      }

      if (this.includeDimensions.isEmpty() || this.includeDimensions.contains(dimension)) {
        futures.add(getContributionDeltaPackedAsync(current, baseline, dimension));
      }
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
}
