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

package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.collect.Multimap;
import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.datasource.MetricExpression;

public interface OLAPDataBaseClient {

  void setCollection(String collection);

  void setMetricExpression(MetricExpression metricExpressions);

  void setBaselineStartInclusive(DateTime dateTime);

  void setBaselineEndExclusive(DateTime dateTime);

  void setCurrentStartInclusive(DateTime dateTime);

  void setCurrentEndExclusive(DateTime dateTime);

  Row getTopAggregatedValues(Multimap<String, String> filterSets) throws Exception;

  List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception;

  List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception;
}
