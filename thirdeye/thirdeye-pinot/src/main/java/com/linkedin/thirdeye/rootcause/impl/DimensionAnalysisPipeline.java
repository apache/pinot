package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.diffsummary.Cube;
import com.linkedin.thirdeye.client.diffsummary.DimNameValueCostEntry;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.OLAPDataBaseClient;
import com.linkedin.thirdeye.client.diffsummary.PinotThirdEyeSummaryClient;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

  public static final String PROP_PARALLELISM = "parallelism";

  public static final long TIMEOUT = 120000;

  private static final String KEY = "key";
  static final String DIMENSION = "dimension";
  static final String COST = "cost";
  static final String VALUE = "value";

  private final QueryCache cache;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final ExecutorService executor;

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
      DatasetConfigManager datasetDAO, QueryCache cache, ExecutorService executor) {
    super(outputName, inputNames);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
    this.executor = executor;
  }

  /**
   * Alternate constructor for use by PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_PARALLELISM=1})
   */
  public DimensionAnalysisPipeline(String outputName, Set<String> inputNames, Map<String, String> properties) {
    super(outputName, inputNames);

    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();

    int parallelism = 1;
    if(properties.containsKey(PROP_PARALLELISM))
      parallelism = Integer.parseInt(properties.get(PROP_PARALLELISM));
    this.executor = Executors.newFixedThreadPool(parallelism);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metricsEntities = context.filter(MetricEntity.class);

    final TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    final TimeRangeEntity baseline = TimeRangeEntity.getContextBaseline(context);

    DataFrame dfScore = new DataFrame();
    dfScore.addSeries(DIMENSION, StringSeries.empty());
    dfScore.addSeries(VALUE, StringSeries.empty());
    dfScore.addSeries(COST, DoubleSeries.empty());

    Map<String, Future<DataFrame>> scores = new HashMap<>();
    for(MetricEntity me : metricsEntities) {
      final MetricConfigDTO metricDTO = metricDAO.findByMetricAndDataset(me.getMetric(), me.getDataset());
      final DatasetConfigDTO datasetDTO = datasetDAO.findByDataset(me.getDataset());

      if(metricDTO == null) {
        LOG.warn("Could not resolve metric '{}'. Skipping.", me.getMetric());
        continue;
      }

      if(datasetDTO == null) {
        LOG.warn("Could not resolve dataset '{}'. Skipping metric '{}'", me.getDataset(), me.getMetric());
        continue;
      }

      // Create asynchronous scoring task
      final MetricEntity entity = me;
      Callable<DataFrame> scoreTask = new Callable<DataFrame>() {
        @Override
        public DataFrame call() throws Exception {
          LOG.info("Scoring metric '{}' in dataset '{}' with weight {}", entity.getMetric(), entity.getDataset(), entity.getScore());
          DataFrame dfMetric = DimensionAnalysisPipeline.this.score(datasetDTO, metricDTO, current, baseline);

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
      scores.put(me.getUrn(), fScore);
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

  /**
   * Perform contribution analysis on a metric given a time range and baseline.
   *
   * @param dataset thirdeye dataset reference
   * @param metric thirdeye metric reference
   * @param current current time range
   * @param baseline baseline time range
   * @return DataFrame with normalized cost
   * @throws Exception if data cannot be fetched or data is invalid
   */
  DataFrame score(DatasetConfigDTO dataset, MetricConfigDTO metric, TimeRangeEntity current, TimeRangeEntity baseline) throws Exception {
    if(!metric.getDataset().equals(dataset.getDataset()))
      throw new IllegalArgumentException("Dataset and metric must match");

    // build data cube
    OLAPDataBaseClient olapClient = getOlapDataBaseClient(current, baseline, metric, dataset);
    Dimensions dimensions = new Dimensions(dataset.getDimensions());
    int topDimensions = dataset.getDimensions().size();

    Cube cube = new Cube();
    cube.buildWithAutoDimensionOrder(olapClient, dimensions, topDimensions, Collections.<List<String>>emptyList());

    return toNormalizedDataFrame(cube.getCostSet());
  }

  private OLAPDataBaseClient getOlapDataBaseClient(TimeRangeEntity current, TimeRangeEntity baseline, MetricConfigDTO metric, DatasetConfigDTO dataset) throws Exception {
    final String timezone = "UTC";
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric.getName(), MetricAggFunction.SUM, dataset.getDataset());

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(cache);
    olapClient.setCollection(dataset.getDataset());
    olapClient.setMetricExpression(metricExpressions.get(0));
    olapClient.setCurrentStartInclusive(new DateTime(current.getStart(), DateTimeZone.forID(timezone)));
    olapClient.setCurrentEndExclusive(new DateTime(current.getEnd(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineStartInclusive(new DateTime(baseline.getStart(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineEndExclusive(new DateTime(baseline.getEnd(), DateTimeZone.forID(timezone)));
    return olapClient;
  }

  private static DataFrame toNormalizedDataFrame(Collection<DimNameValueCostEntry> costs) {
    String[] dim = new String[costs.size()];
    String[] value = new String[costs.size()];
    double[] cost = new double[costs.size()];
    int i = 0;
    for(DimNameValueCostEntry e : costs) {
      dim[i] = e.getDimName();
      value[i] = e.getDimValue();
      cost[i] = e.getCost();
      i++;
    }

    DoubleSeries sCost = DataFrame.toSeries(cost).fillNull();

    DataFrame df = new DataFrame();
    df.addSeries(DIMENSION, dim);
    df.addSeries(VALUE, value);

    if(sCost.sum() > 0.0) {
      df.addSeries(COST, sCost.divide(sCost.sum()));
    } else {
      df.addSeries(COST, sCost);
    }

    return df;
  }
}
