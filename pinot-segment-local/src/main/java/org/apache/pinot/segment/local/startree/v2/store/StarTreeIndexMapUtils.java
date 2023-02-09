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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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

  private static final char KEY_SEPARATOR = '.';
  private static final String KEY_TEMPLATE = "%d.%s.%s.%s";
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
  public static class IndexKey implements Comparable<IndexKey> {
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
      return String.format(KEY_TEMPLATE, starTreeId, _column, _indexType, suffix);
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

    @Override
    public int compareTo(IndexKey other) {
      return Comparator
          .comparing((IndexKey i) -> i._column, Comparator.nullsLast(Comparator.naturalOrder()))
          .thenComparing((IndexKey i) -> i._indexType)
          .compare(this, other);
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
  public static void storeToFile(List<List<Pair<IndexKey, IndexValue>>> indexMaps, File indexMapFile) {
    Preconditions.checkState(!indexMapFile.exists(), "Star-tree index map file already exists");

    PropertiesConfiguration configuration = CommonsConfigurationUtils.fromFile(indexMapFile);
    int numStarTrees = indexMaps.size();
    for (int i = 0; i < numStarTrees; i++) {
      List<Pair<IndexKey, IndexValue>> indexMap = indexMaps.get(i);
      for (Pair<IndexKey, IndexValue> entry : indexMap) {
        IndexKey key = entry.getKey();
        IndexValue value = entry.getValue();
        configuration.addProperty(key.getPropertyName(i, OFFSET_SUFFIX), value._offset);
        configuration.addProperty(key.getPropertyName(i, SIZE_SUFFIX), value._size);
      }
    }
    CommonsConfigurationUtils.saveToFile(configuration, indexMapFile);
  }

  /**
   * Loads the index maps for multiple star-trees from an input stream.
   */
  public static List<Map<IndexKey, IndexValue>> loadFromInputStream(InputStream indexMapInputStream, int numStarTrees) {
    List<Map<IndexKey, IndexValue>> indexMaps = new ArrayList<>(numStarTrees);
    for (int i = 0; i < numStarTrees; i++) {
      indexMaps.add(new HashMap<>());
    }

    PropertiesConfiguration configuration = CommonsConfigurationUtils.fromInputStream(indexMapInputStream);
    for (String key : CommonsConfigurationUtils.getKeys(configuration)) {
      String[] split = StringUtils.split(key, KEY_SEPARATOR);
      int starTreeId = Integer.parseInt(split[0]);
      Map<IndexKey, IndexValue> indexMap = indexMaps.get(starTreeId);

      // Handle the case of column name containing '.'
      String column;
      int columnSplitEndIndex = split.length - 2;
      if (columnSplitEndIndex == 2) {
        column = split[1];
      } else {
        column = StringUtils.join(split, KEY_SEPARATOR, 1, columnSplitEndIndex);
      }

      IndexType indexType = IndexType.valueOf(split[columnSplitEndIndex]);
      IndexKey indexKey;
      if (indexType == IndexType.STAR_TREE) {
        indexKey = STAR_TREE_INDEX_KEY;
      } else {
        indexKey = new IndexKey(IndexType.FORWARD_INDEX, column);
      }
      IndexValue indexValue = indexMap.computeIfAbsent(indexKey, (k) -> new IndexValue());
      long value = configuration.getLong(key);
      if (split[columnSplitEndIndex + 1].equals(OFFSET_SUFFIX)) {
        indexValue._offset = value;
      } else {
        indexValue._size = value;
      }
    }

    return indexMaps;
  }
}
