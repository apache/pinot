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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code StarTreeIndexSeparator} pulls out the individual star-trees from the common star-tree index file
 */
public class StarTreeIndexSeparator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexSeparator.class);

  private final List<Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue>> _indexMapList;
  private final List<StarTreeV2BuilderConfig> _builderConfigList;
  private final List<Integer> _numDocsList;
  private final FileChannel _indexFileChannel;

  public StarTreeIndexSeparator(File indexMapFile, File indexFile, List<StarTreeV2Metadata> starTreeMetadataList)
      throws IOException {
    try (InputStream inputStream = new FileInputStream(indexMapFile)) {
      _indexMapList = StarTreeIndexMapUtils.loadFromInputStream(inputStream, starTreeMetadataList);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
    int numStarTrees = starTreeMetadataList.size();
    _builderConfigList = new ArrayList<>(numStarTrees);
    _numDocsList = new ArrayList<>(numStarTrees);
    for (StarTreeV2Metadata starTreeMetadata : starTreeMetadataList) {
      StarTreeV2BuilderConfig config = StarTreeV2BuilderConfig.fromMetadata(starTreeMetadata);
      _builderConfigList.add(config);
      _numDocsList.add(starTreeMetadata.getNumDocs());
    }
    if (LOGGER.isDebugEnabled()) {
      StringBuilder logExistingStarTrees = new StringBuilder();
      logExistingStarTrees.append("Existing star-tree configs :");
      for (StarTreeV2BuilderConfig config : _builderConfigList) {
        logExistingStarTrees.append("\n").append(config);
      }
      LOGGER.debug(logExistingStarTrees.toString());
    }
    _indexFileChannel = new RandomAccessFile(indexFile, "r").getChannel();
  }

  /**
   * Extract star-tree index files of the star-tree represented by the builderConfig.
   * The extracted star-tree index files are written to the provided starTreeOutputDir.
   *
   * @param starTreeOutputDir output directory for extracted star-trees
   * @param builderConfig {@link StarTreeV2BuilderConfig} of the star-tree to separate
   * @return if star-tree exist then total docs in the separated tree, else -1
   * @throws IOException
   */
  public int separate(File starTreeOutputDir, StarTreeV2BuilderConfig builderConfig)
      throws IOException {
    int treeIndex = _builderConfigList.indexOf(builderConfig);
    if (treeIndex == -1) {
      LOGGER.info("No existing star-tree found for config: {}", builderConfig);
      return -1;
    }
    LOGGER.info("Separating star-tree for config: {}", builderConfig);
    separate(starTreeOutputDir, treeIndex);
    return _numDocsList.get(treeIndex);
  }

  private void separate(File starTreeOutputDir, int treeIndex)
      throws IOException {
    Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue> indexMap = _indexMapList.get(treeIndex);
    FileUtils.forceMkdir(starTreeOutputDir);
    for (StarTreeIndexMapUtils.IndexKey key : indexMap.keySet()) {
      File destIndexFile;
      switch (key._indexType) {
        case STAR_TREE:
          destIndexFile = new File(starTreeOutputDir, StarTreeV2Constants.STAR_TREE_INDEX_FILE_NAME);
          writeIndexToFile(destIndexFile, indexMap.get(key));
          break;
        case FORWARD_INDEX:
          String suffix = key._column.contains(AggregationFunctionColumnPair.DELIMITER)
              ? V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION
              : V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
          destIndexFile = new File(starTreeOutputDir, key._column + suffix);
          writeIndexToFile(destIndexFile, indexMap.get(key));
          break;
        default:
      }
    }
  }

  private void writeIndexToFile(File destFile, StarTreeIndexMapUtils.IndexValue value)
      throws IOException {
    try (FileChannel dest = new RandomAccessFile(destFile, "rw").getChannel()) {
      org.apache.pinot.common.utils.FileUtils.transferBytes(_indexFileChannel, value._offset, value._size, dest);
      dest.force(true);
    }
  }

  @Override
  public void close()
      throws IOException {
    _indexFileChannel.close();
  }
}
