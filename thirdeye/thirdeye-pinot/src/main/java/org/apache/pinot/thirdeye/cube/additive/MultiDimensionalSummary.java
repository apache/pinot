/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.cube.additive;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import java.util.List;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import org.apache.pinot.thirdeye.cube.data.cube.Cube;
import org.apache.pinot.thirdeye.cube.data.dbclient.CubePinotClient;
import org.apache.pinot.thirdeye.cube.summary.Summary;
import org.apache.pinot.thirdeye.cube.summary.SummaryResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


/**
 * A portal class that is used to trigger the multi-dimensional summary algorithm and to get the summary response.
 */
public class MultiDimensionalSummary {
  private CubePinotClient dbClient;
  private CostFunction costFunction;
  private DateTimeZone dateTimeZone;

  public MultiDimensionalSummary(CubePinotClient olapClient, CostFunction costFunction,
      DateTimeZone dateTimeZone) {
    Preconditions.checkNotNull(olapClient);
    Preconditions.checkNotNull(dateTimeZone);
    Preconditions.checkNotNull(costFunction);
    this.dbClient = olapClient;
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

    dbClient.setDataset(dataset);
    ((AdditiveDBClient) dbClient).setMetric(metric);
    dbClient.setCurrentStartInclusive(new DateTime(currentStartInclusive, dateTimeZone));
    dbClient.setCurrentEndExclusive(new DateTime(currentEndExclusive, dateTimeZone));
    dbClient.setBaselineStartInclusive(new DateTime(baselineStartInclusive, dateTimeZone));
    dbClient.setBaselineEndExclusive(new DateTime(baselineEndExclusive, dateTimeZone));

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
    response.setMetricName(metric);

    return response;
  }
}
