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
import java.util.Set;
import java.util.List;
import org.slf4j.Logger;
import java.util.HashSet;
import java.util.ArrayList;
import java.io.IOException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import java.nio.charset.Charset;
import com.google.common.collect.BiMap;
import org.apache.commons.io.FileUtils;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;


public class OffHeapStarTreeV2Builder implements StarTreeV2Builder {

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final Logger LOGGER = LoggerFactory.getLogger(com.linkedin.pinot.core.startree.OffHeapStarTreeBuilder.class);

  private int _nodeCount;
  private TreeNode _rootNode;
  private int _maxNumLeafRecords;
  private List<Integer> _dimensionsSplitOrder;
  private Set<Integer> _dimensionsWithoutStarNode;

  private File _tempDir;
  private File _dataFile;

  // Dimensions

  private int _dimensionSize;
  private int _dimensionCount;
  private final List<String> _dimensionNames = new ArrayList<>();
  private final List<Object> _dimensionStarValues = new ArrayList<>();
  private final List<BiMap<Object, Integer>> _dimensionDictionaries = new ArrayList<>();

  // Metrics
  private int _metricCount;
  private final List<String> _metricNames = new ArrayList<>();

  /**
   * Helper class to represent a tree node.
   */
  private static class TreeNode {
    int _dimensionId = -1;
    int _dimensionValue = -1;
    int _childDimensionId = -1;
    Map<Integer, TreeNode> _children;
    int _startDocId;
    int _endDocId;
    int _aggregatedDocId;
  }

  @Override
  public void init(StarTreeV2BuilderConfig config) throws IOException {
    _tempDir = config.getOutDir();
    if (_tempDir == null) {
      _tempDir = new File(FileUtils.getTempDirectory(), V1Constants.STAR_TREE_INDEX_DIR + "_" + DateTime.now());
    }
    FileUtils.forceMkdir(_tempDir);
    _dataFile = new File(_tempDir, "star-tree.buf");

    LOGGER.info("Star tree temporary directory: {}", _tempDir);
    LOGGER.info("Star tree data file: {}", _dataFile);

    // dimension
    List<DimensionFieldSpec> dimensions = config.getDimensions();
    _dimensionCount = dimensions.size();
    for (DimensionFieldSpec dimension : dimensions) {
        String dimensionName = dimension.getName();
        _dimensionNames.add(dimensionName);
        _dimensionStarValues.add(dimension.getDefaultNullValue());
        _dimensionDictionaries.add(HashBiMap.<Object, Integer>create());
    }
    _dimensionSize = _dimensionCount * V1Constants.Numbers.INTEGER_SIZE;

    List<String> dimensionsSplitOrder = config.getDimensionsSplitOrder();
    if (dimensionsSplitOrder != null) {
      _dimensionsSplitOrder = new ArrayList<>(dimensionsSplitOrder.size());
      for (String dimensionName : dimensionsSplitOrder) {
        _dimensionsSplitOrder.add(_dimensionNames.indexOf(dimensionName));
      }
    }

    // dimensions without star nodes.
    Set<String> dimensionsWithoutStarNode = config.getDimensionsWithoutStarNode();
    if (dimensionsWithoutStarNode != null) {
      _dimensionsWithoutStarNode = getDimensionIdSet(dimensionsWithoutStarNode);
    }

    // Metric
    ListMultimap<MetricFieldSpec, String> metric2aggFuncPairs = config.getMetric2aggFuncPairs();
    _metricCount = metric2aggFuncPairs.size();
    for (MetricFieldSpec metric: metric2aggFuncPairs.keySet()) {
      _metricNames.add(metric.getName());
    }

    LOGGER.info("Dimension Names: {}", _dimensionNames);
    LOGGER.info("Metric Names: {}", _metricNames);

    // Initialize the root node
    _rootNode = new TreeNode();
    _nodeCount++;

    // other initialisation
    _maxNumLeafRecords = config.getMaxNumLeafRecords();
  }

  @Override
  public void append(GenericRow row) throws IOException {

  }

  @Override
  public void build() throws IOException {

  }

  @Override
  public void serialize(File starTreeFile, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap)
      throws IOException {

  }

  @Override
  public List<String> getMetaData() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  private Set<Integer> getDimensionIdSet(Set<String> dimensionNameSet) {
    Set<Integer> dimensionIdSet = new HashSet<>(dimensionNameSet.size());
    for (int i = 0; i < _dimensionCount; i++) {
      if (dimensionNameSet.contains(_dimensionNames.get(i))) {
        dimensionIdSet.add(i);
      }
    }
    return dimensionIdSet;
  }
}
