package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
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
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
 * The MetricComponentAnalysisPipeline performs iterative factor analysis on a metric's dimensions.
 * Returns the k contributors to change (similar to principal components)
 *
 * @see com.linkedin.thirdeye.client.diffsummary.Cube
 */
public class MetricComponentAnalysisPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricComponentAnalysisPipeline.class);

  private static final String COL_RAW = "raw";
  private static final String COL_CONTRIB = "contribution";
  private static final String COL_DELTA = "delta";
  private static final String COL_DIM_NAME = "dimension";
  private static final String COL_DIM_VALUE = "value";
  private static final String COL_SCORE = "score";

  private static final String PROP_PARALLELISM = "parallelism";
  private static final int PROP_PARALLELISM_DEFAULT = 1;

  private static final String PROP_K = "k";
  private static final int PROP_K_DEFAULT = 3;

  private static final String PROP_EXCLUDE_DIMENSIONS = "excludeDimensions";
  private static final Set<String> PROP_EXCLUDE_DIMENSIONS_DEFAULT = Collections.emptySet();

  public static final long TIMEOUT = 120000;

  private final QueryCache cache;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final ExecutorService executor;
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
   */
  public MetricComponentAnalysisPipeline(String outputName, Set<String> inputNames, MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO, QueryCache cache, ExecutorService executor, Set<String> excludeDimensions, int k) {
    super(outputName, inputNames);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
    this.executor = executor;
    this.excludeDimensions = excludeDimensions;
    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K}, {@code PROP_PARALLELISM}, {@code PROP_EXCLUDE_DIMENSIONS})
   */
  public MetricComponentAnalysisPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    this.executor = Executors.newFixedThreadPool(MapUtils.getInteger(properties, PROP_PARALLELISM, PROP_PARALLELISM_DEFAULT));
    this.k = MapUtils.getInteger(properties, PROP_K, PROP_K_DEFAULT);

    if (properties.containsKey(PROP_EXCLUDE_DIMENSIONS)) {
      this.excludeDimensions = new HashSet<>((Collection<String>) properties.get(PROP_EXCLUDE_DIMENSIONS));
    } else {
      this.excludeDimensions = PROP_EXCLUDE_DIMENSIONS_DEFAULT;
    }
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metricsEntities = context.filter(MetricEntity.class);

    final TimeRangeEntity anomaly = TimeRangeEntity.getTimeRangeAnomaly(context);
    final TimeRangeEntity baseline = TimeRangeEntity.getTimeRangeBaseline(context);

    final Multimap<String, String> rawDimensions = HashMultimap.create();
    final Set<DimensionEntity> dimensions = new MaxScoreSet<>();

    if (metricsEntities.isEmpty()) {
      return new PipelineResult(context, new MaxScoreSet<>());
    }

    if (metricsEntities.size() > 1) {
      // NOTE: emergency brake, expensive computation and queries
      throw new IllegalArgumentException("Cannot process more than one metric at a time");
    }

    final MetricEntity metric = metricsEntities.iterator().next();

    // collects filters over multiple iterations
    final Multimap<String, String> filters = HashMultimap.create(metric.getFilters());

    for (int k = 0; k < this.k; k++) {
      try {
        final MetricSlice sliceCurrent = MetricSlice.from(metric.getId(), anomaly.getStart(), anomaly.getEnd(), filters);
        final MetricSlice sliceBaseline = MetricSlice.from(metric.getId(), baseline.getStart(), baseline.getEnd(), filters);

        final DataFrame dfScoresRaw = getDimensionScores(sliceCurrent, sliceBaseline);

        LOG.info("Iteration {}: analyzing '{}'\n{}", k, filters, dfScoresRaw.head(50));

        // ignore zero scores, known combinations
        final DataFrame dfScores = dfScoresRaw
            .filter(new Series.DoubleConditional() {
              @Override
              public boolean apply(double... values) {
                return values[0] > 0;
              }
            }, COL_SCORE)
            .filter(new Series.StringConditional() {
              @Override
              public boolean apply(String... values) {
                return !rawDimensions.containsEntry(values[0], values[1]);
              }
            }, COL_DIM_NAME, COL_DIM_VALUE)
            .dropNull();

        if (dfScores.isEmpty()) {
          break;
        }

        // TODO ignore common attributes (e.g. "continent=North America" for "country=us")

        String name = dfScores.getString(COL_DIM_NAME, 0);
        String value = dfScores.getString(COL_DIM_VALUE, 0);
        double score = dfScores.getDouble(COL_SCORE, 0);

        rawDimensions.put(name, value);
        dimensions.add(DimensionEntity.fromDimension(score * metric.getScore(), name, value, DimensionEntity.TYPE_GENERATED));

        // NOTE: workaround for ThirdEye's lack of exclusion query ("dimName != dimValue")
        if (!filters.keySet().contains(name)) {
          Set<String> allValues = new HashSet<>(dfScoresRaw.filterEquals(COL_DIM_NAME, name).dropNull().getStrings(COL_DIM_VALUE).unique().toList());
          filters.putAll(name, allValues);
        }
        filters.remove(name, value);

      } catch (Exception e) {
        LOG.warn("Error calculating dimension scores for '{}'. Skipping.", metric.getUrn(), e);
      }
    }

    return new PipelineResult(context, dimensions);
  }

  private DataFrame getContribution(MetricSlice slice, String dimension) throws Exception {
    String ref = String.format("%d-%s", slice.getMetricId(), dimension);
    RequestContainer rc = DataFrameUtils.makeAggregateRequest(slice, Collections.singletonList(dimension), ref, this.metricDAO, this.datasetDAO);
    ThirdEyeResponse res = this.cache.getQueryResult(rc.getRequest());

    DataFrame raw = DataFrameUtils.evaluateResponse(res, rc);

    DataFrame out = new DataFrame();
    out.addSeries(dimension, raw.getStrings(dimension));
    out.addSeries(COL_CONTRIB, raw.getDoubles(DataFrameUtils.COL_VALUE).normalizeSum());
    out.addSeries(COL_RAW, raw.getDoubles(DataFrameUtils.COL_VALUE));
    out.setIndex(dimension);

    return out;
  }

  private DataFrame getContributionDelta(MetricSlice current, MetricSlice baseline, String dimension) throws Exception {
    DataFrame curr = getContribution(current, dimension);
    DataFrame base = getContribution(baseline, dimension);

    DataFrame joined = curr.joinOuter(base)
        .fillNull(COL_CONTRIB + DataFrame.COLUMN_JOIN_LEFT)
        .fillNull(COL_CONTRIB + DataFrame.COLUMN_JOIN_RIGHT)
        .fillNull(COL_RAW + DataFrame.COLUMN_JOIN_LEFT)
        .fillNull(COL_RAW + DataFrame.COLUMN_JOIN_RIGHT);

    DoubleSeries diff = joined.getDoubles(COL_CONTRIB + DataFrame.COLUMN_JOIN_LEFT)
        .subtract(joined.getDoubles(COL_CONTRIB + DataFrame.COLUMN_JOIN_RIGHT));

    DoubleSeries diffRaw = joined.getDoubles(COL_RAW + DataFrame.COLUMN_JOIN_LEFT)
        .subtract(joined.getDoubles(COL_RAW + DataFrame.COLUMN_JOIN_RIGHT));

    DataFrame df = new DataFrame();
    df.addSeries(dimension, joined.getStrings(dimension));
    df.addSeries(COL_CONTRIB, joined.getDoubles(COL_CONTRIB + DataFrame.COLUMN_JOIN_LEFT));
    df.addSeries(COL_DELTA, diff);
    df.addSeries(COL_RAW, diffRaw);
    df.setIndex(dimension);

    return df;
  }

  private DataFrame packDimension(DataFrame dfDelta, String dimension) {
    DataFrame df = new DataFrame();
    df.addSeries(COL_CONTRIB, dfDelta.get(COL_CONTRIB));
    df.addSeries(COL_DELTA, dfDelta.get(COL_DELTA));
    df.addSeries(COL_RAW, dfDelta.get(COL_RAW));
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
      // don't explore dimensions that are excluded
      if (this.excludeDimensions.contains(dimension)) {
        continue;
      }

      futures.add(getContributionDeltaPackedAsync(current, baseline, dimension));
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
        COL_DELTA + ":DOUBLE",
        COL_RAW + ":DOUBLE").build();

    combined = combined.append(contributors.toArray(new DataFrame[contributors.size()]));
    combined.addSeries(COL_SCORE, combined.getDoubles(COL_DELTA).abs());

    return combined.sortedBy(COL_SCORE).reverse();
  }
}
