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

import com.google.common.collect.Lists;
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
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;


/**
 * The {@code StarTreeIndexSeparator} pulls out the individual star-trees from the common star-tree index file
 */
public class StarTreeIndexSeparator implements Closeable {

  private final FileChannel _indexFileChannel;
  private final List<Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue>> _indexMapList;
  private final List<StarTreeV2BuilderConfig> _builderConfigList;
  private final List<Integer> _totalDocsList;

  public StarTreeIndexSeparator(File indexMapFile, File indexFile, PropertiesConfiguration metadataProperties)
      throws IOException {
    _indexMapList = extractIndexMap(indexMapFile,
        metadataProperties.getInt(StarTreeV2Constants.MetadataKey.STAR_TREE_COUNT));
    _indexFileChannel = new RandomAccessFile(indexFile, "r").getChannel();
    _builderConfigList = extractBuilderConfigs(metadataProperties);
    _totalDocsList = extractTotalDocsList(metadataProperties);
  }

  private List<Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue>> extractIndexMap(File indexMapFile,
      int numStarTrees) {
    try (InputStream inputStream = new FileInputStream(indexMapFile)) {
      return StarTreeIndexMapUtils.loadFromInputStream(inputStream, numStarTrees);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Integer> extractTotalDocsList(PropertiesConfiguration metadataProperties) {
    List<Integer> totalDocsList = new ArrayList<>(_indexMapList.size());
    for (int i = 0; i < _indexMapList.size(); i++) {
      Configuration metadata = metadataProperties.subset(StarTreeV2Constants.MetadataKey.getStarTreePrefix(i));
      totalDocsList.add(i, metadata.getInt(StarTreeV2Constants.MetadataKey.TOTAL_DOCS));
    }
    return totalDocsList;
  }

  /**
   * Extract the list of {@link StarTreeV2BuilderConfig} for each of the star-tree present in the given metadata
   * properties.
   * @param metadataProperties index metadata properties
   * @return List of {@link StarTreeV2BuilderConfig}
   */
  public List<StarTreeV2BuilderConfig> extractBuilderConfigs(PropertiesConfiguration metadataProperties) {
    List<StarTreeV2BuilderConfig> builderConfigList = new ArrayList<>(_indexMapList.size());
    for (int i = 0; i < _indexMapList.size(); i++) {
      Configuration metadata = metadataProperties.subset(StarTreeV2Constants.MetadataKey.getStarTreePrefix(i));
      builderConfigList.add(i, StarTreeV2BuilderConfig.fromIndexConfig(new StarTreeIndexConfig(
          Lists.newArrayList(metadata.getStringArray(StarTreeV2Constants.MetadataKey.DIMENSIONS_SPLIT_ORDER)),
          Lists.newArrayList(
              metadata.getStringArray(StarTreeV2Constants.MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS)),
          Lists.newArrayList(metadata.getStringArray(StarTreeV2Constants.MetadataKey.FUNCTION_COLUMN_PAIRS)),
          metadata.getInt(StarTreeV2Constants.MetadataKey.MAX_LEAF_RECORDS))));
    }
    return builderConfigList;
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
      return -1;
    }
    separate(starTreeOutputDir, treeIndex);
    return _totalDocsList.get(treeIndex);
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
