package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.diffsummary.costfunctions.CostFunction;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.diffsummary.Summary;
import com.linkedin.thirdeye.dashboard.views.diffsummary.SummaryResponse;
import com.linkedin.thirdeye.datasource.MetricExpression;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


/**
 * A portal class that is used to trigger the multi-dimensional summary algorithm and to get the summary response.
 */
public class MultiDimensionalSummary {
  private OLAPDataBaseClient olapClient;
  private CostFunction costFunction;
  private DateTimeZone dateTimeZone;

  public MultiDimensionalSummary(OLAPDataBaseClient olapClient, CostFunction costFunction,
      DateTimeZone dateTimeZone) {
    Preconditions.checkNotNull(olapClient);
    Preconditions.checkNotNull(dateTimeZone);
    Preconditions.checkNotNull(costFunction);
    this.olapClient = olapClient;
    this.costFunction = costFunction;
    this.dateTimeZone = dateTimeZone;
  }

  /**
   * Builds the summary given the given metric information.
   *
   * @param dataset the dataset of the metric.
   * @param metric the name of the metric.
   * @param currentStartInclusive the start time of current data cube, inclusive.
   * @param currentEndExclusive the end time of the current data cube, exclusive.
   * @param baselineStartInclusive the start of the baseline data cube, inclusive.
   * @param baselineEndExclusive the end of the baseline data cube, exclusive.
   * @param dimensions the dimensions to be considered in the summary. If the variable depth is zero, then the order
   *                   of the dimension is used; otherwise, this method will determine the order of the dimensions
   *                   depending on their cost. After the order is determined, the first 'depth' dimensions are used
   *                   the generated the summary.
   * @param dataFilters the filter to be applied on the data cube.
   * @param summarySize the number of entries to be put in the summary.
   * @param depth the number of dimensions to be drilled down when analyzing the summary.
   * @param hierarchies the hierarchy among the dimensions. The order will always be honored when determining the order
   *                    of dimensions.
   * @param doOneSideError if the summary should only consider one side error.
   *
   * @return the multi-dimensional summary.
   */
  public SummaryResponse buildSummary(String dataset, String metric, long currentStartInclusive,
      long currentEndExclusive, long baselineStartInclusive, long baselineEndExclusive, Dimensions dimensions,
      Multimap<String, String> dataFilters, int summarySize, int depth, List<List<String>> hierarchies,
      boolean doOneSideError) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(metric));
    Preconditions.checkArgument(currentStartInclusive < currentEndExclusive);
    Preconditions.checkArgument(baselineStartInclusive < baselineEndExclusive);
    Preconditions.checkNotNull(dimensions);
    Preconditions.checkArgument(dimensions.size() > 0);
    Preconditions.checkNotNull(dataFilters);
    Preconditions.checkArgument(summarySize > 1);
    Preconditions.checkNotNull(hierarchies);
    Preconditions.checkArgument(depth >= 0);

    olapClient.setCollection(dataset);
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric, MetricAggFunction.SUM, dataset);
    olapClient.setMetricExpression(metricExpressions.get(0));
    olapClient.setCurrentStartInclusive(new DateTime(currentStartInclusive, dateTimeZone));
    olapClient.setCurrentEndExclusive(new DateTime(currentEndExclusive, dateTimeZone));
    olapClient.setBaselineStartInclusive(new DateTime(baselineStartInclusive, dateTimeZone));
    olapClient.setBaselineEndExclusive(new DateTime(baselineEndExclusive, dateTimeZone));

    Cube cube = new Cube(costFunction);
    SummaryResponse response;
    if (depth > 0) { // depth != 0 means manual dimension order
      cube.buildWithAutoDimensionOrder(olapClient, dimensions, dataFilters, depth, hierarchies);
      Summary summary = new Summary(cube, costFunction);
      response = summary.computeSummary(summarySize, doOneSideError, depth);
    } else { // manual dimension order
      cube.buildWithManualDimensionOrder(olapClient, dimensions, dataFilters);
      Summary summary = new Summary(cube, costFunction);
      response = summary.computeSummary(summarySize, doOneSideError);
    }
    response.setDataset(dataset);
    response.setMetricName(metric);

    return response;
  }
}
