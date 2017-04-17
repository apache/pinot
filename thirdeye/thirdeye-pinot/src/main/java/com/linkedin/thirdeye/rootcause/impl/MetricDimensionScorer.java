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
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricDimensionScorer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricDimensionScorer.class);

  private static final String DIMENSION = "dimension";
  private static final String CONTRIBUTION = "contribution";

  final QueryCache cache;

  public MetricDimensionScorer(QueryCache cache) {
    this.cache = cache;
  }

  Collection<MetricDimensionEntity> score(Collection<MetricDimensionEntity> entities, TimeRangeEntity current, BaselineEntity baseline) throws Exception {
    if(entities.isEmpty())
      return Collections.emptyList();

    // ensure same base dataset and metric
    Iterator<MetricDimensionEntity> it = entities.iterator();
    MetricDimensionEntity first = it.next();
    MetricConfigDTO metric = first.getMetric();
    DatasetConfigDTO dataset = first.getDataset();

    while(it.hasNext()) {
      MetricDimensionEntity e = it.next();
      if(!metric.equals(e.getMetric()))
        throw new IllegalArgumentException("entities must derive from same metric");
      if(!dataset.equals(e.getDataset()))
        throw new IllegalArgumentException("entities must derive from same dataset");
    }

    // build data cube
    OLAPDataBaseClient olapClient = getOlapDataBaseClient(current, baseline, metric, dataset);
    Dimensions dimensions = new Dimensions(dataset.getDimensions());
    int topDimensions = dataset.getDimensions().size();

    Cube cube = new Cube();
    cube.buildWithAutoDimensionOrder(olapClient, dimensions, topDimensions, Collections.<List<String>>emptyList());

    // group by dimension
    DataFrame df = makeContribution(cube.getCostSet());

    // map dimension to MetricDimension
    Map<String, MetricDimensionEntity> mdMap = new HashMap<>();
    for(MetricDimensionEntity e : entities) {
      mdMap.put(e.getDimension(), e);
    }

    List<MetricDimensionEntity> scores = new ArrayList<>();
    for(int i=0; i<df.size(); i++) {
      MetricDimensionEntity e = mdMap.get(df.getString(Series.GROUP_KEY, i));
      MetricDimensionEntity n = e.withScore(df.getDouble(Series.GROUP_VALUE, i));
      scores.add(n);
    }

    return scores;
  }

  private OLAPDataBaseClient getOlapDataBaseClient(TimeRangeEntity current, BaselineEntity baseline, MetricConfigDTO metric, DatasetConfigDTO dataset) throws Exception {
    final String timezone = "UTC";
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric.getName(), MetricAggFunction.SUM, dataset.getDataset());

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(cache);
    olapClient.setCollection(dataset.getDataset());
    olapClient.setMetricExpression(metricExpressions.get(0));
    olapClient.setCurrentStartInclusive(new DateTime(current.getStart(), DateTimeZone.forID(timezone)));
    olapClient.setCurrentEndExclusive(new DateTime(current.getEnd(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineStartInclusive(new DateTime(baseline.getEnd(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineEndExclusive(new DateTime(baseline.getEnd(), DateTimeZone.forID(timezone)));
    return olapClient;
  }

  private static DataFrame makeContribution(Collection<DimNameValueCostEntry> costs) {
    String[] dim = new String[costs.size()];
    double[] contrib = new double[costs.size()];
    int i = 0;
    for(DimNameValueCostEntry e : costs) {
      dim[i] = e.getDimName();
      contrib[i] = e.getContributionFactor();
      i++;
    }

    DataFrame df = new DataFrame();
    df.addSeries(DIMENSION, dim);
    df.addSeries(CONTRIBUTION, contrib);
    df.setIndex(DIMENSION);

    DataFrame agg = df.groupBy(DIMENSION).aggregate(CONTRIBUTION, DoubleSeries.SUM);
    agg.addSeries(Series.GROUP_KEY, agg.getDoubles(Series.GROUP_VALUE).normalize());
    return agg;
  }

//  private static DataFrame aggregateContribution(DataFrame aggregate, DataFrame df, final BaselineEntity baseline) {
//    DoubleSeries s = df.getDoubles(CONTRIBUTION).normalize();
//    s = s.map(new DoubleSeries.DoubleFunction() {
//      @Override
//      public double apply(double... values) {
//        return values[0] * baseline.getScore();
//      }
//    });
//
//    DataFrame normalized = new DataFrame(df);
//    normalized.addSeries(CONTRIBUTION, s);
//
//    DataFrame joined = aggregate.joinOuter(normalized);
//    DoubleSeries left = joined.getDoubles(CONTRIBUTION).fillNull();
//    DoubleSeries right = joined.getDoubles(CONTRIBUTION + DataFrame.COLUMN_JOIN_POSTFIX).fillNull();
//
//    DataFrame out = new DataFrame(aggregate);
//    out.addSeries(DIMENSION, left.add(right));
//
//    return out;
//  }
}
