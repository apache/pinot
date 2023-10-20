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
package org.apache.pinot.segment.spi.index.startree;

/**
 * The {@code StarTreeV2Constants} class contains all the constant values used by star-tree.
 */
public class StarTreeV2Constants {
  private StarTreeV2Constants() {
  }

  // File names after combining
  public static final String INDEX_FILE_NAME = "star_tree_index";
  public static final String INDEX_MAP_FILE_NAME = "star_tree_index_map";

  // File names before combining
  public static final String STAR_TREE_TEMP_DIR = "star_tree_tmp";
  public static final String STAR_TREE_INDEX_FILE_NAME = "star_tree.index";
  public static final String EXISTING_STAR_TREE_TEMP_DIR = "existing_star_tree_tmp";

  // NOTE: because of bit compression, we cannot store -1 for star in forward index. Because star value should never be
  // accessed, we can simply put 0 as the place holder
  public static final int STAR_IN_FORWARD_INDEX = 0;

  // Metadata keys
  public static class MetadataKey {
    public static final String STAR_TREE_SUBSET = "startree.v2";
    public static final String STAR_TREE_PREFIX = STAR_TREE_SUBSET + '.';
    public static final String STAR_TREE_COUNT = STAR_TREE_PREFIX + "count";

    public static final String TOTAL_DOCS = "total.docs";
    public static final String DIMENSIONS_SPLIT_ORDER = "split.order";
    public static final String FUNCTION_COLUMN_PAIRS = "function.column.pairs";
    public static final String AGGREGATION_CONFIG = "aggregation.config";
    public static final String MAX_LEAF_RECORDS = "max.leaf.records";
    public static final String SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS = "skip.star.node.creation";
    public static final String FUNCTION_TYPE = "function.type";
    public static final String COLUMN_NAME = "column.name";
    public static final String COMPRESSION_TYPE = "compression.type";
    public static final String NUM_OF_AGGREGATION_CONFIG = "num.of.aggregation.config";

    public static String getStarTreePrefix(int index) {
      return STAR_TREE_PREFIX + index;
    }
  }
}
