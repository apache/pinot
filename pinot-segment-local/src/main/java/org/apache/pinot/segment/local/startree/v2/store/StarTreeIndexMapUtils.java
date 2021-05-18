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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;


/**
 * The {@code StarTreeIndexMapUtils} class is a utility class to store/load star-tree index map to/from file.
 * <p>
 * The star-tree index map file contains index maps for multiple star-trees, where the index map is a map from index to
 * the offset and size in the star-tree index file.
 * <p>
 * The entry in the index file is stored in the following format (for STAR_TREE index type, column is null):
 * <ul>
 *   <li>$starTreeId.$column.$indexType.OFFSET = $indexOffsetInFile</li>
 *   <li>$starTreeId.$column.$indexType.SIZE = $indexSize</li>
 * </ul>
 * <p>
 * For an example:
 * <ul>
 *   <li>0.null.STAR_TREE.OFFSET = 0</li>
 *   <li>0.null.STAR_TREE.SIZE = 2000</li>
 *   <li>0.dimension1.FORWARD.OFFSET = 2000</li>
 *   <li>0.dimension1.FORWARD.SIZE = 1000</li>
 *   <li>0.dimension2.FORWARD.OFFSET = 3000</li>
 *   <li>0.dimension2.FORWARD.SIZE = 1500</li>
 *   <li>0.sum__metric.FORWARD.OFFSET = 4500</li>
 *   <li>0.sum__metric.FORWARD.SIZE = 1000</li>
 *   <li>1.null.STAR_TREE.OFFSET = 5500</li>
 *   <li>1.null.STAR_TREE.SIZE = 2500</li>
 *   <li>...</li>
 * </ul>
 */
public class StarTreeIndexMapUtils {
  private StarTreeIndexMapUtils() {
  }

  public static final IndexKey STAR_TREE_INDEX_KEY = new IndexKey(IndexType.STAR_TREE, null);

  private static final String OFFSET_SUFFIX = "OFFSET";
  private static final String SIZE_SUFFIX = "SIZE";

  /**
   * Type of the index.
   */
  public enum IndexType {
    STAR_TREE, FORWARD_INDEX
  }

  /**
   * Key of the index map.
   */
  public static class IndexKey {
    public final IndexType _indexType;
    // For star-tree index, column will be null
    public final String _column;

    public IndexKey(IndexType indexType, @Nullable String column) {
      _indexType = indexType;
      _column = column;
    }

    /**
     * Returns the property name for the index.
     */
    public String getPropertyName(int starTreeId, String suffix) {
      return String.format("%d.%s.%s.%s", starTreeId, _column, _indexType, suffix);
    }

    @Override
    public int hashCode() {
      return 31 * _indexType.hashCode() + Objects.hashCode(_column);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (obj instanceof IndexKey) {
        IndexKey that = (IndexKey) obj;
        return _indexType == that._indexType && Objects.equals(_column, that._column);
      } else {
        return false;
      }
    }
  }

  /**
   * Value of the index map.
   */
  public static class IndexValue implements Comparable<IndexValue> {
    public long _offset;
    public long _size;

    public IndexValue() {
    }

    public IndexValue(long offset, long size) {
      _offset = offset;
      _size = size;
    }

    @Override
    public int compareTo(@Nonnull IndexValue o) {
      return Long.compare(_offset, o._offset);
    }
  }

  /**
   * Stores the index maps for multiple star-trees into a file.
   */
  public static void storeToFile(List<Map<IndexKey, IndexValue>> indexMaps, File indexMapFile)
      throws ConfigurationException {
    Preconditions.checkState(!indexMapFile.exists(), "Star-tree index map file already exists");

    PropertiesConfiguration configuration = new PropertiesConfiguration(indexMapFile);
    int numStarTrees = indexMaps.size();
    for (int i = 0; i < numStarTrees; i++) {
      Map<IndexKey, IndexValue> indexMap = indexMaps.get(i);
      for (Map.Entry<IndexKey, IndexValue> entry : indexMap.entrySet()) {
        IndexKey key = entry.getKey();
        IndexValue value = entry.getValue();
        configuration.addProperty(key.getPropertyName(i, OFFSET_SUFFIX), value._offset);
        configuration.addProperty(key.getPropertyName(i, SIZE_SUFFIX), value._size);
      }
    }
    
    // Commons Configuration 1.10 does not support file path containing '%'. 
    // Explicitly providing the output stream for the file bypasses the problem.       
    try (FileOutputStream fileOutputStream = new FileOutputStream(configuration.getFile())) {
      configuration.save(fileOutputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the index maps for multiple star-trees from a file.
   */
  public static List<Map<IndexKey, IndexValue>> loadFromFile(File indexMapFile, int numStarTrees)
      throws ConfigurationException {
    Preconditions.checkState(indexMapFile.exists(), "Star-tree index map file does not exist");

    PropertiesConfiguration configuration = CommonsConfigurationUtils.fromFile(indexMapFile);
    
    List<Map<IndexKey, IndexValue>> indexMaps = IntStream.range(0, numStarTrees).boxed()
        .map(index -> new HashMap<IndexKey, IndexValue>()).collect(Collectors.toList());
    
    for (String key : CommonsConfigurationUtils.getKeys(configuration)) {
      String[] split = key.split("\\.");
      Preconditions.checkState(split.length == 4,
          "Invalid key: " + key + " in star-tree index map file: " + indexMapFile.getAbsolutePath());
      int starTreeId = Integer.parseInt(split[0]);
      Map<IndexKey, IndexValue> indexMap = indexMaps.get(starTreeId);
      IndexType indexType = IndexType.valueOf(split[2]);
      IndexKey indexKey;
      if (indexType == IndexType.STAR_TREE) {
        indexKey = STAR_TREE_INDEX_KEY;
      } else {
        indexKey = new IndexKey(IndexType.FORWARD_INDEX, split[1]);
      }
      IndexValue indexValue = indexMap.computeIfAbsent(indexKey, (k) -> new IndexValue());
      long value = configuration.getLong(key);
      if (split[3].equals(OFFSET_SUFFIX)) {
        indexValue._offset = value;
      } else {
        indexValue._size = value;
      }
    }
    return indexMaps;
  }
}
