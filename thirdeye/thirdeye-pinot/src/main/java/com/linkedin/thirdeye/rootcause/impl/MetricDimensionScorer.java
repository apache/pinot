package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.diffsummary.Cube;
import com.linkedin.thirdeye.client.diffsummary.DimNameValueCostEntry;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.OLAPDataBaseClient;
import com.linkedin.thirdeye.client.diffsummary.PinotThirdEyeSummaryClient;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.rootcause.SearchContext;
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

  Map<MetricDimensionEntity, Double> score(Collection<MetricDimensionEntity> entities, SearchContext context) throws Exception {
    if(entities.isEmpty())
      return Collections.emptyMap();

    // ensure same base dataset and metric
    Iterator<MetricDimensionEntity> it = entities.iterator();
    MetricDimensionEntity first = it.next();
    DatasetConfigDTO dataset = first.getDataset();
    MetricConfigDTO metric = first.getMetric();

    while(it.hasNext()) {
      MetricDimensionEntity e = it.next();
      if(!dataset.equals(e.getDataset()))
        throw new IllegalArgumentException("entities must derive from same dataset");
      if(!metric.equals(e.getMetric()))
        throw new IllegalArgumentException("entities must derive from same metric");
    }

    // build data cube
    OLAPDataBaseClient olapClient = getOlapDataBaseClient(context, dataset, metric);
    Dimensions dimensions = new Dimensions(dataset.getDimensions());
    int topDimensions = dataset.getDimensions().size();

    Cube cube = new Cube();
    cube.buildWithAutoDimensionOrder(olapClient, dimensions, topDimensions, Collections.<List<String>>emptyList());

    // group by dimension
    DataFrame df = makeContibutionFrame(cube.getCostSet());

    // map dimension to MetricDimension
    Map<String, MetricDimensionEntity> mdMap = new HashMap<>();
    for(MetricDimensionEntity e : entities) {
      mdMap.put(e.getDimension(), e);
    }

    Map<MetricDimensionEntity, Double> scores = new HashMap<>();
    for(int i=0; i<df.size(); i++) {
      MetricDimensionEntity e = mdMap.get(df.getString(Series.GROUP_KEY, i));
      scores.put(e, df.getDouble(Series.GROUP_VALUE, i));
    }

    return scores;
  }

  private OLAPDataBaseClient getOlapDataBaseClient(SearchContext context, DatasetConfigDTO dataset,
      MetricConfigDTO metric) {
    String timezone = "UTC";
    List<MetricExpression> metricExpressions = new ArrayList<>();
    metricExpressions.add(new MetricExpression(MetricConfigBean.DERIVED_METRIC_ID_PREFIX + metric.getId(), dataset.getDataset()));

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(cache);
    olapClient.setCollection(dataset.getDataset());
    olapClient.setMetricExpression(metricExpressions.get(0));
    olapClient.setCurrentStartInclusive(new DateTime(context.getTimestampStart(), DateTimeZone.forID(timezone)));
    olapClient.setCurrentEndExclusive(new DateTime(context.getTimestampEnd(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineStartInclusive(new DateTime(context.getBaselineStart(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineEndExclusive(new DateTime(context.getTimestampEnd(), DateTimeZone.forID(timezone)));
    return olapClient;
  }

  private static DataFrame makeContibutionFrame(Collection<DimNameValueCostEntry> costs) {
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

    return df.groupBy(DIMENSION).aggregate(CONTRIBUTION, DoubleSeries.SUM);
  }
}
