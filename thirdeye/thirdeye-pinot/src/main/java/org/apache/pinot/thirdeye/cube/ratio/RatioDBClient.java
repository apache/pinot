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

package org.apache.pinot.thirdeye.cube.ratio;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.dbclient.CubeTag;
import org.apache.pinot.thirdeye.cube.data.dbclient.BaseCubePinotClient;
import org.apache.pinot.thirdeye.cube.data.dbclient.CubeSpec;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;


/**
 * This class generates query requests to the backend database and retrieve the metrics that compose the ratio metric
 * for summary algorithm.
 *
 * @see org.apache.pinot.thirdeye.cube.data.dbclient.BaseCubePinotClient
 */
public class RatioDBClient extends BaseCubePinotClient<RatioRow> {
  private String numeratorMetric = "";
  private String denominatorMetric = "";

  /**
   * Constructs a DB client to the ratio metric.
   *
   * @param queryCache the query cache to Pinot DB.
   */
  public RatioDBClient(QueryCache queryCache) {
    super(queryCache);
  }

  /**
   * Sets the numerator metric of the ratio metric.
   *
   * @param numeratorMetric the numerator metric of the ratio metric.
   */
  public void setNumeratorMetric(String numeratorMetric) {
    this.numeratorMetric = numeratorMetric;
  }

  /**
   * Sets the denominator metric of the ratio metric.
   *
   * @param denominatorMetric the denominator metric of the ratio metric.
   */
  public void setDenominatorMetric(String denominatorMetric) {
    this.denominatorMetric = denominatorMetric;
  }

  @Override
  protected List<CubeSpec> getCubeSpecs() {
    List<CubeSpec> cubeSpecs = new ArrayList<>();

    cubeSpecs.add(
        new CubeSpec(CubeTag.BaselineNumerator, numeratorMetric, baselineStartInclusive, baselineEndExclusive));
    cubeSpecs.add(
        new CubeSpec(CubeTag.BaselineDenominator, denominatorMetric, baselineStartInclusive, baselineEndExclusive));
    cubeSpecs.add(new CubeSpec(CubeTag.CurrentNumerator, numeratorMetric, currentStartInclusive, currentEndExclusive));
    cubeSpecs.add(
        new CubeSpec(CubeTag.CurrentDenominator, denominatorMetric, currentStartInclusive, currentEndExclusive));

    return cubeSpecs;
  }

  @Override
  protected void fillValueToRowTable(Map<List<String>, RatioRow> rowTable, Dimensions dimensions,
      List<String> dimensionValues, double value, CubeTag tag) {

    if (Double.compare(0d, value) < 0 && !Double.isInfinite(value)) {
      RatioRow row = rowTable.get(dimensionValues);
      if (row == null) {
        row = new RatioRow(dimensions, new DimensionValues(dimensionValues));
        rowTable.put(dimensionValues, row);
      }
      switch (tag) {
        case BaselineNumerator:
          row.setBaselineNumeratorValue(value);
          break;
        case BaselineDenominator:
          row.setBaselineDenominatorValue(value);
          break;
        case CurrentNumerator:
          row.setCurrentNumeratorValue(value);
          break;
        case CurrentDenominator:
          row.setCurrentDenominatorValue(value);
          break;
        default:
      }
    }
  }
}
