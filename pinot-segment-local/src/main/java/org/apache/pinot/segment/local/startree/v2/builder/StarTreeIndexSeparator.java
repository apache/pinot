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
  public static final String STARTREE_SEPARATOR_PREFIX = "startree_";

  private final FileChannel _indexFileChannel;
  private final List<Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue>> _indexMapList;

  public StarTreeIndexSeparator(File indexMapFile, File indexFile, int numStarTrees)
      throws IOException {
    _indexMapList = extractIndexMap(indexMapFile, numStarTrees);
    _indexFileChannel = new RandomAccessFile(indexFile, "r").getChannel();
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
          (List<String>) metadata.getProperty(StarTreeV2Constants.MetadataKey.DIMENSIONS_SPLIT_ORDER),
          (List<String>) metadata.getProperty(StarTreeV2Constants.MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS),
          (List<String>) metadata.getProperty(StarTreeV2Constants.MetadataKey.FUNCTION_COLUMN_PAIRS),
          metadata.getInt(StarTreeV2Constants.MetadataKey.MAX_LEAF_RECORDS))));
    }
    return builderConfigList;
  }

  /**
   * Extract separate star-tree index files for each of the star-tree present in the provided index file.
   * The extracted star-trees are written to the provided starTreeOutputDir.
   * @param starTreeOutputDir output directory for extracted star-trees
   * @throws IOException
   */
  public void separate(File starTreeOutputDir)
      throws IOException {
    for (int i = 0; i < _indexMapList.size(); i++) {
      Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue> indexMap = _indexMapList.get(i);
      File currentTreeDir = new File(starTreeOutputDir, STARTREE_SEPARATOR_PREFIX + i);
      FileUtils.forceMkdir(currentTreeDir);
      for (StarTreeIndexMapUtils.IndexKey key : indexMap.keySet()) {
        File destIndexFile;
        switch (key._indexType) {
          case STAR_TREE:
            destIndexFile = new File(currentTreeDir, StarTreeV2Constants.STAR_TREE_INDEX_FILE_NAME);
            writeIndexToFile(destIndexFile, indexMap.get(key));
            break;
          case FORWARD_INDEX:
            String suffix = key._column.contains(AggregationFunctionColumnPair.DELIMITER)
                ? V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION
                : V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
            destIndexFile = new File(currentTreeDir, key._column + suffix);
            writeIndexToFile(destIndexFile, indexMap.get(key));
            break;
          default:
        }
      }
    }
  }

  private void writeIndexToFile(File destFile, StarTreeIndexMapUtils.IndexValue value)
      throws IOException {
    try (FileChannel dest = new RandomAccessFile(destFile, "rw").getChannel()) {
      org.apache.pinot.common.utils.FileUtils.transferBytes(_indexFileChannel, value._offset, value._size, dest);
    }
  }

  @Override
  public void close()
      throws IOException {
    _indexFileChannel.close();
  }
}
