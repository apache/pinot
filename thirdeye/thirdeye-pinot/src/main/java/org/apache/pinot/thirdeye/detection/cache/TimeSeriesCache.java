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

package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;


/**
 * Loads data from either the data source or the centralized cache.
 */

public interface TimeSeriesCache {

  /**
   * Returns a ThirdEyeResponse object that contains the desired time-series data associated
   * with the request. Attempts to first pull the desired data from the centralized cache, and if
   * it is not in the cache, will request it from the original data source.
   * @param request ThirdEyeRequest object that contains all the info to build a query
   * @return ThirdEyeResponse with time-series rows
   * @throws Exception
   */
  public ThirdEyeResponse fetchTimeSeries(ThirdEyeRequest request) throws Exception;


  /**
   * Takes in a time-series pulled from the original data source, and stores
   * it into the centralized cache.
   * @param response a response object containing the timeseries to be inserted
   */
  public void insertTimeSeriesIntoCache(ThirdEyeResponse response);
}
