/**
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
package org.apache.pinot.segment.local.startree.v2.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


/**
 * The {@code StarTreeIndexContainer} class contains the indexes for multiple star-trees.
 */
public class StarTreeIndexContainer implements Closeable {
  private final List<StarTreeV2> _starTrees;

  public StarTreeIndexContainer(SegmentDirectory.Reader segmentReader, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> indexContainerMap)
      throws IOException {
    _starTrees = StarTreeLoaderUtils.loadStarTreeV2(segmentReader, segmentMetadata, indexContainerMap);
  }

  public List<StarTreeV2> getStarTrees() {
    return _starTrees;
  }

  @Override
  public void close()
      throws IOException {
    // The startree index data buffer is owned by segment reader, but still need to close those startree instances as
    // they have created their own fwd value readers.
    for (StarTreeV2 starTree : _starTrees) {
      starTree.close();
    }
  }
}
