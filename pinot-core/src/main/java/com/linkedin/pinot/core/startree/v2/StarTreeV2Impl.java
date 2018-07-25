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

package com.linkedin.pinot.core.startree.v2;

import java.util.Map;
import java.io.IOException;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.startree.StarTree;


public class StarTreeV2Impl implements StarTreeV2 {

  private final StarTree _starTree;
  private final StarTreeV2Metadata _starTreeV2Metadata;
  private final Map<String, StarTreeV2DimensionDataSource> _dimensionDataSources;
  private final Map<String, StarTreeV2AggfunColumnPairDataSource> _aggFuncColumnPairSources;

  public StarTreeV2Impl(StarTree starTree, StarTreeV2Metadata starTreeV2Metadata, Map<String, StarTreeV2DimensionDataSource> dimensionDataSources,
      Map<String, StarTreeV2AggfunColumnPairDataSource> aggFuncColumnPairSources) {

    _starTree = starTree;
    _starTreeV2Metadata = starTreeV2Metadata;
    _dimensionDataSources = dimensionDataSources;
    _aggFuncColumnPairSources = aggFuncColumnPairSources;
  }

  @Override
  public StarTree getStarTree() {
    return _starTree;
  }

  @Override
  public StarTreeV2Metadata getMetadata() {
    return _starTreeV2Metadata;
  }

  @Override
  public DataSource getDataSource(String column) {
      return _dimensionDataSources.get(column);
  }

  @Override
  public DataSource getDataSource(AggregationFunctionColumnPair aggregationFunctionColumnPair) {
    String pair = aggregationFunctionColumnPair.getFunctionType().getName() + "_" + aggregationFunctionColumnPair.getColumnName();
    return _aggFuncColumnPairSources.get(pair);
  }

  @Override
  public void close() throws IOException {
    if (_starTree != null) {
      _starTree.close();
    }

    if (_dimensionDataSources.size() > 0) {
      for (String dimension: _dimensionDataSources.keySet()) {
        StarTreeV2DimensionDataSource source = _dimensionDataSources.get(dimension);
        source.getForwardIndex().close();
        source.getDictionary().close();
      }
    }

    if (_aggFuncColumnPairSources.size() > 0) {
      for (String pair: _aggFuncColumnPairSources.keySet()) {
        StarTreeV2AggfunColumnPairDataSource source = _aggFuncColumnPairSources.get(pair);
        source.getForwardIndex().close();
      }
    }
  }
}
