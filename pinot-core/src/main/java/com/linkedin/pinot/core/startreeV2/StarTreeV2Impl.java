/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startreeV2;

import java.util.Map;
import java.io.IOException;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.startree.StarTree;


public class StarTreeV2Impl implements StarTreeV2 {

  private final StarTree _starTree;
  private final Map<String, StarTreeV2DimensionDataSource> _dimensionDataSources;
  private final Map<String, StarTreeV2AggfuncColumnPairDataSource> _metricaggFuncPairSources;

  public StarTreeV2Impl(StarTree starTree, Map<String, StarTreeV2DimensionDataSource> dimensionDataSources,
      Map<String, StarTreeV2AggfuncColumnPairDataSource> metricaggFuncPairSources) {

    _starTree = starTree;
    _dimensionDataSources = dimensionDataSources;
    _metricaggFuncPairSources = metricaggFuncPairSources;
  }

  @Override
  public StarTree getStarTree() throws IOException {
    return _starTree;
  }

  @Override
  public DataSource getDataSource(String column) throws Exception {
    if (_dimensionDataSources.containsKey(column)) {
      return _dimensionDataSources.get(column);
    } else {
      return _metricaggFuncPairSources.get(column);
    }
  }
}
