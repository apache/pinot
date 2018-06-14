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

import com.linkedin.pinot.core.startree.StarTreeDataTable;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.List;
import org.slf4j.Logger;
import java.util.ArrayList;
import java.io.IOException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;
import java.nio.charset.Charset;
import java.io.DataOutputStream;
import com.google.common.collect.BiMap;
import org.apache.commons.io.FileUtils;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.startree.MetricBuffer;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.core.startree.DimensionBuffer;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


public class OffHeapStarTreeV2Builder implements StarTreeV2Builder {

  // Dimensions
  private int _dimensionSize;
  private int _dimensionsCount;
  private List<Integer> _dimensionsSplitOrder;
  private List<DimensionFieldSpec> _dimensions;
  private Set<Integer> _dimensionsWithoutStarNode;
  private final List<String> _dimensionsName = new ArrayList<>();
  private final List<BiMap<Object, Integer>> _dimensionsDictionaries = new ArrayList<>();

  // Metrics
  private int _metricSize;
  private int _metricsCount;
  private final List<String> _metricsName = new ArrayList<>();
  private ListMultimap<MetricFieldSpec, String> _met2aggfuncPairs;

  // General
  private File _tempDir;
  private File _dataFile;
  private int _nodesCount;
  private int[] _sortOrder;
  private int _rawDocsCount;
  private TreeNode _rootNode;
  private int _maxNumLeafRecords;
  private int _aggregatedDocsCount;
  private DataOutputStream _dataOutputStream;
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final Logger LOGGER = LoggerFactory.getLogger(com.linkedin.pinot.core.startree.OffHeapStarTreeBuilder.class);

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
    _dimensions = config.getDimensions();
    _dimensionsCount = _dimensions.size();
    for (DimensionFieldSpec dimension : _dimensions) {
        String dimensionName = dimension.getName();
        _dimensionsName.add(dimensionName);
    }
    _dimensionSize = _dimensionsCount * V1Constants.Numbers.INTEGER_SIZE;

    // dimension split order
    List<String> dimensionsSplitOrder = config.getDimensionsSplitOrder();
    if (dimensionsSplitOrder != null) {
      _dimensionsSplitOrder = new ArrayList<>(dimensionsSplitOrder.size());
      for (String dimensionName : dimensionsSplitOrder) {
        _dimensionsSplitOrder.add(_dimensionsName.indexOf(dimensionName));
      }
    }

    // dimensions without star nodes
    Set<String> dimensionsWithoutStarNode = config.getDimensionsWithoutStarNode();
    if (dimensionsWithoutStarNode != null) {
      _dimensionsWithoutStarNode =  OffHeapStarTreeV2BuilderHelper.getDimensionIdSet(dimensionsWithoutStarNode,
          _dimensionsName, _dimensionsCount);
    }

    // metric
    _met2aggfuncPairs = config.getMetric2aggFuncPairs();
    _metricsCount = _met2aggfuncPairs.size();
    for (MetricFieldSpec metric: _met2aggfuncPairs.keys()) {
      _metricsName.add(metric.getName());
      _metricSize += metric.getFieldSize();
    }

    LOGGER.info("Dimension Names: {}", _dimensionsName);
    LOGGER.info("Metric Names: {}", _metricsName);

    // initialize the root node
    _rootNode = new TreeNode();
    _nodesCount++;

    // other initialisation
    _maxNumLeafRecords = config.getMaxNumLeafRecords();

  }

  @Override
  public void append(GenericRow row) throws IOException {
    DimensionBuffer dimensions = new DimensionBuffer(_dimensionsCount);
    for (int i = 0; i < _dimensionsCount; i++) {
      String dimensionName = _dimensionsName.get(i);
      Object dimensionValue = row.getValue(dimensionName);
      dimensions.setDimension(i, dimensionValue);
    }

    // Metrics
    Object[] metricValues = new Object[_metricsCount];
    for (int i = 0; i < _metricsCount; i++) {
      String metricName = _metricsName.get(i);
      Object metricValue = row.getValue(metricName);
      metricValues[i] = metricValue;
    }

    List<MetricFieldSpec> metricFieldSpecList = OffHeapStarTreeV2BuilderHelper.getMetricFieldSpec(_met2aggfuncPairs);
    MetricBuffer metrics = new MetricBuffer(metricValues, metricFieldSpecList);
    appendToRawBuffer(dimensions, metrics);
  }

  @Override
  public void build() throws IOException {


    // compute the dimension split order if it's null.
    if (_dimensionsSplitOrder == null || _dimensionsSplitOrder.isEmpty()) {
      _dimensionsSplitOrder = OffHeapStarTreeV2BuilderHelper.computeDefaultSplitOrder(_dimensionsCount, _dimensionsDictionaries);
    }

    LOGGER.info("Split Order: {}", _dimensionsSplitOrder);

    // Recursively construct the star tree
    constructStarTree(_rootNode, 0, _rawDocsCount, 0);
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



  // helper functions
  private void appendToRawBuffer(DimensionBuffer dimensions, MetricBuffer metrics) throws IOException {
    appendToBuffer(dimensions, metrics);
    _rawDocsCount++;
  }

  private void appendToBuffer(DimensionBuffer dimensions, MetricBuffer metricHolder) throws IOException {
    for (int i = 0; i < _dimensionsCount; i++) {
      _dataOutputStream.writeInt(OffHeapStarTreeV2BuilderHelper.flipEndiannessIfNeeded(dimensions.getDimension(i)));
    }
    _dataOutputStream.write(metricHolder.toBytes(_metricSize));
  }
}
