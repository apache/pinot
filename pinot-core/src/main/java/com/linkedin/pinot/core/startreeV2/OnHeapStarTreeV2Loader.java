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

import java.io.File;
import java.util.Map;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.StarTreeV2Metadata;
import org.apache.commons.configuration.ConfigurationException;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;


public class OnHeapStarTreeV2Loader {

  // segment
  private static IndexLoadingConfig _v3IndexLoadingConfig;
  private static SegmentMetadataImpl _segmentMetadataImpl;

  // star tree
  private static File _indexDir;
  private static List<StarTreeV2Metadata> _starTreeV2MetadataList;
  private static List<StarTreeV2> _starTreeV2DataSources;

  public static List<StarTreeV2> load(File indexDir) throws IOException, ConfigurationException {
    // segment
    _indexDir = indexDir;
    _v3IndexLoadingConfig = new IndexLoadingConfig();
    _v3IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v3IndexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    _segmentMetadataImpl = new SegmentMetadataImpl(indexDir);

    // star tree
    _starTreeV2MetadataList = _segmentMetadataImpl.getStarTreeV2Metadata();
    _starTreeV2DataSources = new ArrayList<>();

    int starTreeId = 0;
    for (StarTreeV2Metadata metaData : _starTreeV2MetadataList) {
      StarTreeV2DataSource dataSource = new StarTreeV2DataSource(_segmentMetadataImpl, metaData, _indexDir);
      StarTree starTree = dataSource.loadStarTree(starTreeId);
      dataSource.loadColumnsDataSource(starTreeId);
      Map<String, StarTreeV2DimensionDataSource> dimensionDataSourceMap = dataSource.getDimensionForwardIndexReader();
      Map<String, StarTreeV2AggfuncColumnPairDataSource> metricAggfuncPairDataSourceMap =
          dataSource.getMetricRawIndexReader();

      StarTreeV2Impl impl = new StarTreeV2Impl(starTree, dimensionDataSourceMap, metricAggfuncPairDataSourceMap);
      _starTreeV2DataSources.add(impl);
      starTreeId += 1;
    }

    return _starTreeV2DataSources;
  }
}
