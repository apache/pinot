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
 * This class generates query requests to the backend database and retrieve the additive metric for summary algorithm.
 *
 * @see org.apache.pinot.thirdeye.cube.data.dbclient.BaseCubePinotClient
 */
public class AdditiveDBClient extends BaseCubePinotClient<AdditiveRow> {
  private String metric;

  /**
   * Constructs a DB client to an additive metric.
   *
   * @param queryCache the query cache to Pinot DB.
   */
  public AdditiveDBClient(QueryCache queryCache) {
    super(queryCache);
  }

  /**
   * Sets the additive metric name.
   *
   * @param metric the additive metric name.
   */
  public void setMetric(String metric) {
    this.metric = Preconditions.checkNotNull(metric);
  }

  public String getMetric() {
    return metric;
  }

  @Override
  protected List<CubeSpec> getCubeSpecs() {
    List<CubeSpec> cubeSpecs = new ArrayList<>();

    cubeSpecs.add(new CubeSpec(CubeTag.Baseline, metric, baselineStartInclusive, baselineEndExclusive));
    cubeSpecs.add(new CubeSpec(CubeTag.Current, metric, currentStartInclusive, currentEndExclusive));

    return cubeSpecs;
  }

  @Override
  protected void fillValueToRowTable(Map<List<String>, AdditiveRow> rowTable, Dimensions dimensions,
      List<String> dimensionValues, double value, CubeTag tag) {

    if (Double.compare(0d, value) < 0 && !Double.isInfinite(value)) {
      AdditiveRow row = rowTable.get(dimensionValues);
      if (row == null) {
        row = new AdditiveRow(dimensions, new DimensionValues(dimensionValues));
        rowTable.put(dimensionValues, row);
      }
      switch (tag) {
        case Baseline:
          row.setBaselineValue(value);
          break;
        case Current:
          row.setCurrentValue(value);
          break;
        default:
          throw new IllegalArgumentException("Unsupported CubeTag: " + tag.name());
      }
    }
  }
}
