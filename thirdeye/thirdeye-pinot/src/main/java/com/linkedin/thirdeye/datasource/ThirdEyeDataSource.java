/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource;

import java.util.List;
import java.util.Map;

public interface ThirdEyeDataSource {

  /**
   * Returns simple name of the class
   */
  String getName();

  ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception;

  List<String> getDatasets() throws Exception;

  /** Clear any cached values. */
  void clear() throws Exception;

  void close() throws Exception;

  /**
   * Returns max dateTime in millis for the dataset
   * @param dataset
   * @return
   * @throws Exception
   */
  long getMaxDataTime(String dataset) throws Exception;

  /**
   * Returns map of dimension name to dimension values for filters
   * @param dataset
   * @return dimension map
   * @throws Exception
   */
  Map<String, List<String>> getDimensionFilters(String dataset) throws Exception;

}
