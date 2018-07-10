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


public class StarTreeV2Constant {

  // final strings
  public static final int STAR_NODE = -1;
  public static final int INVALID_INDEX = -1;
  public static final String STAR_TREE = "startree";
  public static final String STAR_TREE_V2_COUNT = "startreeV2.count";
  public static final String STAR_TREE_V2_ENABLED = "startreeV2.enabled";

  public static final String STAR_TREE_V2_COlUMN_FILE = "startreev2.column.psf";
  public static final String STAR_TREE_V2_INDEX_MAP_FILE = "startreev2.index.map";


  public static final String METRIC_RAW_INDEX_SUFFIX = ".sv.raw.fwd";
  public static final String DIMENSION_FWD_INDEX_SUFFIX = ".sv.unsorted.fwd";

  // aggregate functions name.
  public static class AggregateFunctions {
    public static final String MAX = "max";
    public static final String SUM = "sum";
    public static final String MIN = "min";
  }


  // star tree meta data.
  public static class StarTreeMetadata {
    public static final String STAR_TREE_SPLIT_ORDER = "split.order";
    public static final String STAR_TREE_MAT2FUNC_MAP = "met2func.map";
    public static final String STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS = "skip.star.node.creation.for.dimensions";
  }
}
