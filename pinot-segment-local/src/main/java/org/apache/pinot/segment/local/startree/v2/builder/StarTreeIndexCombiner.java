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
package org.apache.pinot.segment.local.startree.v2.builder;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;

import static org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils.IndexKey;
import static org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils.IndexType;
import static org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils.IndexValue;
import static org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils.STAR_TREE_INDEX_KEY;


/**
 * The {@code StarTreeIndexCombiner} class combines multiple star-tree indexes into a single index file.
 */
public class StarTreeIndexCombiner implements Closeable {
  private final FileChannel _fileChannel;

  public StarTreeIndexCombiner(File indexFile)
      throws IOException {
    Preconditions.checkState(!indexFile.exists(), "Star-tree index file already exists");
    _fileChannel = new RandomAccessFile(indexFile, "rw").getChannel();
  }

  /**
   * Combines the index files inside the given directory into the single index file, then cleans the directory.
   */
  public List<Pair<IndexKey, IndexValue>> combine(StarTreeV2BuilderConfig builderConfig, File starTreeIndexDir)
      throws IOException {
    List<Pair<IndexKey, IndexValue>> indexMap = new ArrayList<>();

    // Write star-tree index
    File starTreeIndexFile = new File(starTreeIndexDir, StarTreeV2Constants.STAR_TREE_INDEX_FILE_NAME);
    indexMap.add(Pair.of(STAR_TREE_INDEX_KEY, writeFile(starTreeIndexFile)));

    // Write dimension indexes
    for (String dimension : builderConfig.getDimensionsSplitOrder()) {
      File dimensionIndexFile =
          new File(starTreeIndexDir, dimension + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
      indexMap.add(Pair.of(new IndexKey(IndexType.FORWARD_INDEX, dimension), writeFile(dimensionIndexFile)));
    }

    // Write metric (function-column pair) indexes
    for (Map.Entry<String, AggregationFunctionColumnPair> functionColumnPair : builderConfig.getFunctionColumnPairs()
        .entrySet()) {
      String metric = functionColumnPair.getValue().toColumnName();
      File metricIndexFile =
          new File(starTreeIndexDir, metric + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      indexMap.add(Pair.of(new IndexKey(IndexType.FORWARD_INDEX, metric), writeFile(metricIndexFile)));
    }

    FileUtils.cleanDirectory(starTreeIndexDir);
    return indexMap;
  }

  private IndexValue writeFile(File srcFile)
      throws IOException {
    try (FileChannel src = new RandomAccessFile(srcFile, "r").getChannel()) {
      long offset = _fileChannel.position();
      long size = src.size();
      org.apache.pinot.common.utils.FileUtils.transferBytes(src, 0, size, _fileChannel);
      return new IndexValue(offset, size);
    }
  }

  @Override
  public void close()
      throws IOException {
    _fileChannel.close();
  }
}
