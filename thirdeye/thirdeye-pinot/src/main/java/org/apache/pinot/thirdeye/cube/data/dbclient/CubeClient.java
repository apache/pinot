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

package org.apache.pinot.thirdeye.cube.data.dbclient;

import com.google.common.collect.Multimap;
import java.util.List;

import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.dbrow.Row;


/**
 * The database client that provides the function of data retrieval for the cube algorithm.
 * @param <R>
 */
public interface CubeClient<R extends Row> {

  /**
   * Returns the baseline and current value for the root node.
   *
   * @param filterSets the data filter.
   * @return a row of data that contains the baseline and current value for the root node.
   */
  R getTopAggregatedValues(Multimap<String, String> filterSets) throws Exception;

  /**
   * Returns the baseline and current value for nodes at each dimension from the given list.
   * For instance, if the list has ["country", "page name"], then it returns nodes of ["US", "IN", "JP", ...,
   * "linkedin.com", "google.com", ...]
   *
   * @param dimensions the list of dimensions.
   * @param filterSets the data filter.
   *
   * @return the baseline and current value for nodes at each dimension from the given list.
   */
  List<List<R>> getAggregatedValuesOfDimension(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception;

  /**
   * Returns the baseline and current value for nodes for each dimension combination.
   * For instance, if the list has ["country", "page name"], then it returns nodes of
   * [
   *   ["US", "IN", "JP", ...,],
   *   ["US, linkedin.com", "US, google.com", "IN, linkedin.com", "IN, google.com", "JP, linkedin.com", "JP, google.com", ...]
   * ]
   * @param dimensions the dimensions to be drilled down.
   * @param filterSets the data filter.
   *
   * @return the baseline and current value for nodes for each dimension combination.
   */
  List<List<R>> getAggregatedValuesOfLevels(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception;
}
