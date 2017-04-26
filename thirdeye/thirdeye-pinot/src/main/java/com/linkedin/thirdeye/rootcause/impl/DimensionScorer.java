package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.MetricExpression;
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
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The DimensionScorer performs contribution analysis for sets of associated metric
 * dimensions given a time range and a baseline range. It relies on ThirdEye's internal QueryCache
 * to obtain the necessary data for constructing a data cube.
 */
public class DimensionScorer {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionScorer.class);

  static final String DIMENSION = "dimension";
  static final String COST = "cost";
  static final String VALUE = "value";

  private final QueryCache cache;

  public DimensionScorer(QueryCache cache) {
    this.cache = cache;
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
