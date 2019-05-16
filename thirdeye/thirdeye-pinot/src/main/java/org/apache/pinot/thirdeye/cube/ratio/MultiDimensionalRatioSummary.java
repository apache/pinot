package org.apache.pinot.thirdeye.cube.ratio;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.cube.data.cube.Cube;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import org.apache.pinot.thirdeye.cube.summary.Summary;
import org.apache.pinot.thirdeye.cube.summary.SummaryResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiDimensionalRatioSummary {
  private static final Logger LOG = LoggerFactory.getLogger(MultiDimensionalRatioSummary.class);

  private RatioCubePinotClient dbClient;
  private CostFunction costFunction;
  private DateTimeZone dateTimeZone;

  public MultiDimensionalRatioSummary(RatioCubePinotClient dbClient, CostFunction costFunction, DateTimeZone dateTimeZone) {
    Preconditions.checkNotNull(dbClient);
    Preconditions.checkNotNull(dateTimeZone);
    Preconditions.checkNotNull(costFunction);

    this.dbClient = dbClient;
    this.costFunction = costFunction;
    this.dateTimeZone = dateTimeZone;
  }

  public SummaryResponse buildRatioSummary(String dataset, String numeratorMetric, String denominatorMetric,
      long currentStartInclusive, long currentEndExclusive, long baselineStartInclusive, long baselineEndExclusive,
      Dimensions dimensions, Multimap<String, String> dataFilters, int summarySize, int depth,
      List<List<String>> hierarchies, boolean doOneSideError) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(numeratorMetric));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(denominatorMetric));
    Preconditions.checkArgument(currentStartInclusive < currentEndExclusive);
    Preconditions.checkArgument(baselineStartInclusive < baselineEndExclusive);
    Preconditions.checkNotNull(dimensions);
    Preconditions.checkArgument(dimensions.size() > 0);
    Preconditions.checkNotNull(dataFilters);
    Preconditions.checkArgument(summarySize > 1);
    Preconditions.checkNotNull(hierarchies);
    Preconditions.checkArgument(depth >= 0);


    dbClient.setDataset(dataset);
    dbClient.setNumeratorMetric(numeratorMetric);
    dbClient.setDenominatorMetric(denominatorMetric);
    dbClient.setCurrentStartInclusive(new DateTime(currentStartInclusive, dateTimeZone));
    dbClient.setCurrentEndExclusive(new DateTime(currentEndExclusive, dateTimeZone));
    dbClient.setBaselineStartInclusive(new DateTime(baselineStartInclusive, dateTimeZone));
    dbClient.setBaselineEndExclusive(new DateTime(baselineEndExclusive, dateTimeZone));

    LOG.info(new DateTime(baselineStartInclusive, dateTimeZone).toString());
    LOG.info(new DateTime(baselineEndExclusive, dateTimeZone).toString());
    LOG.info(new DateTime(currentStartInclusive, dateTimeZone).toString());
    LOG.info(new DateTime(currentEndExclusive, dateTimeZone).toString());

    RatioRow topAggregatedValues = dbClient.getTopAggregatedValues(null);
    LOG.info(topAggregatedValues.toString());

    List<List<RatioRow>> aggregatedValuesOfDimension = dbClient.getAggregatedValuesOfDimension(new Dimensions(
        Collections.singletonList("use_case")), null);
    LOG.info(aggregatedValuesOfDimension.toString());

    Cube cube = new Cube(costFunction);
    SummaryResponse response;
    if (depth > 0) { // depth != 0 means manual dimension order
      cube.buildWithAutoDimensionOrder(dbClient, dimensions, dataFilters, depth, hierarchies);
      Summary summary = new Summary(cube, costFunction);
      response = summary.computeSummary(summarySize, doOneSideError, depth);
    } else { // manual dimension order
      cube.buildWithManualDimensionOrder(dbClient, dimensions, dataFilters);
      Summary summary = new Summary(cube, costFunction);
      response = summary.computeSummary(summarySize, doOneSideError);
    }
    response.setDataset(dataset);
    response.setMetricName(numeratorMetric + "/" + denominatorMetric);

    return response;
  }
}
