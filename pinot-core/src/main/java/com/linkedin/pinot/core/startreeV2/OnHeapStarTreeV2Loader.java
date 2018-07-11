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
import com.linkedin.pinot.common.segment.StarTreeV2Metadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


public class OnHeapStarTreeV2Loader implements StarTreeV2Loader {

  // segment
  private SegmentMetadataImpl _segmentMetadata;

  // star tree
  private int _starTreesCount;
  private File _starTreeIndexDataFile;
  private String _starTreeIndexMapFile;
  private Map<String, Integer> _starTreeIndexMetadata;
  private List<StarTreeV2Metadata> _starTreeV2MetadataList;
  private List<StarTreeV2DataSource> _starTreeV2DataSources;

  @Override
  public void init(File indexDir) throws Exception {
    // segment
    _segmentMetadata = new SegmentMetadataImpl(indexDir);

    // star tree
    _starTreesCount = _segmentMetadata.getStarTreeV2Count();
    _starTreeV2MetadataList = _segmentMetadata.getStarTreeV2Metadata();
    _starTreeIndexDataFile = new File(indexDir, StarTreeV2Constant.STAR_TREE_V2_COlUMN_FILE);
    _starTreeIndexMapFile = new File(indexDir, StarTreeV2Constant.STAR_TREE_V2_INDEX_MAP_FILE).getPath();
  }

  @Override
  public void load() throws IOException {
    _starTreeIndexMetadata = OnHeapStarTreeV2LoaderHelper.readMetaData(_starTreeIndexMapFile);
    _starTreeV2DataSources = new ArrayList<>();

    int starTreeId = 0;
    for (StarTreeV2Metadata metaData: _starTreeV2MetadataList) {
      StarTreeV2DataSource a = new StarTreeV2DataSource(_segmentMetadata, metaData, _starTreeIndexMetadata, _starTreeIndexDataFile);
      a.loadDataSource(starTreeId);
      _starTreeV2DataSources.add(a);
      starTreeId += 1;
    }
    return;
  }

  @Override
  public StarTreeV2DataSource returnDataSource(int starTreeId) throws Exception {
    return _starTreeV2DataSources.get(starTreeId);
  }
}
