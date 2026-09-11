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
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils.TreeNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DataType.BYTES;


/// The `BaseSingleTreeBuilder` class is the base class for star-tree builders that works on a single
/// [StarTreeV2BuilderConfig]s and provides common methods to build a single star-tree.
@SuppressWarnings({"rawtypes", "unchecked"})
abstract class BaseSingleTreeBuilder implements SingleTreeBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSingleTreeBuilder.class);

  final StarTreeV2BuilderConfig _builderConfig;
  final File _outputDir;
  final ImmutableSegment _segment;
  final Configuration _metadataProperties;

  final int _numDimensions;
  final String[] _dimensionsSplitOrder;
  final Set<Integer> _skipStarNodeCreationForDimensions;
  final PinotSegmentColumnReader[] _dimensionReaders;
  // Cardinality of each dimension's segment dictionary. In a null-aware star-tree this doubles as the dictionary id
  // reserved for null values, which is why the dimension forward index is then created with cardinality + 1 values.
  // Reserving the id one past the last real one keeps nulls sorted after every real value, so they form their own
  // tree node instead of folding into the column's default null value.
  final int[] _dimensionCardinalities;

  final int _numMetrics;
  // Name of the function-column pairs
  final String[] _metrics;
  final ValueAggregator[] _valueAggregators;
  // Readers and data types for column in function-column pair
  final PinotSegmentColumnReader[] _metricReaders;
  final AggregationSpec[] _aggregationSpecs;

  final int _maxLeafRecords;
  final boolean _nullHandlingEnabled;

  final TreeNode _rootNode = getNewNode();

  int _numDocs;
  int _numNodes;

  /// The `Record` class represents a record (raw or aggregated) with dimension dictionary Ids and metric values.
  static class Record {
    final int[] _dimensions;
    final Object[] _metrics;

    Record(int[] dimensions, Object[] metrics) {
      _dimensions = dimensions;
      _metrics = metrics;
    }
  }

  /// Constructor for the base single star-tree builder.
  ///
  /// @param builderConfig Builder config
  /// @param outputDir Directory to store the index files
  /// @param segment Index segment
  /// @param metadataProperties Segment metadata properties
  BaseSingleTreeBuilder(StarTreeV2BuilderConfig builderConfig, File outputDir, ImmutableSegment segment,
      Configuration metadataProperties) {
    _builderConfig = builderConfig;
    _outputDir = outputDir;
    _segment = segment;
    _metadataProperties = metadataProperties;
    _nullHandlingEnabled = builderConfig.isNullHandlingEnabled();

    List<String> dimensionsSplitOrder = builderConfig.getDimensionsSplitOrder();
    _numDimensions = dimensionsSplitOrder.size();
    _dimensionsSplitOrder = new String[_numDimensions];
    _skipStarNodeCreationForDimensions = new HashSet<>();
    _dimensionReaders = new PinotSegmentColumnReader[_numDimensions];
    _dimensionCardinalities = new int[_numDimensions];
    Set<String> skipStarNodeCreationForDimensions = builderConfig.getSkipStarNodeCreationForDimensions();
    for (int i = 0; i < _numDimensions; i++) {
      String dimension = dimensionsSplitOrder.get(i);
      _dimensionsSplitOrder[i] = dimension;
      if (skipStarNodeCreationForDimensions.contains(dimension)) {
        _skipStarNodeCreationForDimensions.add(i);
      }
      _dimensionReaders[i] = new PinotSegmentColumnReader(segment, dimension);
      Preconditions.checkState(_dimensionReaders[i].hasDictionary(),
          "Dimension: " + dimension + " does not have dictionary");
      _dimensionCardinalities[i] = segment.getDictionary(dimension).length();
    }

    TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs = builderConfig.getAggregationSpecs();
    _numMetrics = aggregationSpecs.size();
    _metrics = new String[_numMetrics];
    _valueAggregators = new ValueAggregator[_numMetrics];
    _metricReaders = new PinotSegmentColumnReader[_numMetrics];
    _aggregationSpecs = new AggregationSpec[_numMetrics];

    int index = 0;
    for (Map.Entry<AggregationFunctionColumnPair, AggregationSpec> entry : aggregationSpecs.entrySet()) {
      AggregationFunctionColumnPair functionColumnPair = entry.getKey();
      AggregationSpec aggregationSpec = entry.getValue();
      _metrics[index] = functionColumnPair.toColumnName();
      List<ExpressionContext> arguments = StarTreeBuilderUtils.expressionContextFromFunctionParameters(
          functionColumnPair.getFunctionType(), aggregationSpec.getFunctionParameters());
      _valueAggregators[index] =
          ValueAggregatorFactory.getValueAggregator(functionColumnPair.getFunctionType(), arguments);
      _aggregationSpecs[index] = aggregationSpec;
      // COUNT(*) counts rows rather than values and needs no reader. A null-aware star-tree can additionally store
      // COUNT(column), which counts the non-null values of that column and therefore does need one.
      String column = functionColumnPair.getColumn();
      if (_valueAggregators[index].getAggregationType() != AggregationFunctionType.COUNT || !column.equals(
          AggregationFunctionColumnPair.STAR)) {
        _metricReaders[index] = new PinotSegmentColumnReader(segment, column);
      }

      index++;
    }

    _maxLeafRecords = builderConfig.getMaxLeafRecords();
  }

  /// Appends a record to the star-tree.
  ///
  /// @param record Record to be appended
  abstract void appendRecord(Record record)
      throws IOException;

  /// Returns the record of the given document Id in the star-tree.
  ///
  /// @param docId Document Id
  /// @return Star-tree record
  abstract Record getStarTreeRecord(int docId)
      throws IOException;

  /// Returns the dimension value of the given document and dimension Id in the star-tree.
  ///
  /// @param docId Document Id
  /// @param dimensionId Dimension Id
  /// @return Dimension value
  abstract int getDimensionValue(int docId, int dimensionId)
      throws IOException;

  /// Sorts and aggregates the records in the segment, and returns a record iterator for all the aggregated records.
  ///
  /// This method reads records from segment and generates the initial records for the star-tree.
  ///
  /// @param numDocs Number of documents in the segment
  /// @return Iterator for the aggregated records
  abstract Iterator<Record> sortAndAggregateSegmentRecords(int numDocs)
      throws IOException;

  /// Generates aggregated records for star-node.
  ///
  /// This method will do the following steps:
  ///
  /// - Creates a temporary buffer for the given range of documents
  /// - Replaces the value for the given dimension Id to `STAR`
  /// - Sorts the records inside the temporary buffer
  /// - Aggregates the records with same dimensions
  /// - Returns an iterator for the aggregated records
  ///
  /// @param startDocId Start document Id in the star-tree
  /// @param endDocId End document Id (exclusive) in the star-tree
  /// @param dimensionId Dimension Id of the star-node
  /// @return Iterator for the aggregated records
  abstract Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId)
      throws IOException;

  /// Reads the dimensions for a record of the given document Id in the segment.
  ///
  /// @param docId Document Id
  /// @return Dimensions (dictionary Ids) for a segment record
  int[] getSegmentRecordDimensions(int docId) {
    int[] dimensions = new int[_numDimensions];
    if (_nullHandlingEnabled) {
      for (int i = 0; i < _numDimensions; i++) {
        PinotSegmentColumnReader dimensionReader = _dimensionReaders[i];
        // A null value is stored under the reserved dictionary id instead of the dictionary id of the column's
        // default null value, so that null rows are not grouped together with rows holding that default value
        dimensions[i] = dimensionReader.isNull(docId) ? _dimensionCardinalities[i] : dimensionReader.getDictId(docId);
      }
    } else {
      for (int i = 0; i < _numDimensions; i++) {
        dimensions[i] = _dimensionReaders[i].getDictId(docId);
      }
    }
    return dimensions;
  }

  /// Reads a record of the given document Id in the segment.
  ///
  /// @param docId Document Id
  /// @return Segment record
  Record getSegmentRecord(int docId) {
    int[] dimensions = getSegmentRecordDimensions(docId);
    Object[] metrics = new Object[_numMetrics];
    for (int i = 0; i < _numMetrics; i++) {
      // Ignore the column for COUNT(*), which has no reader
      PinotSegmentColumnReader metricReader = _metricReaders[i];
      if (metricReader != null) {
        // A null-aware star-tree excludes null values from the aggregation, so it passes them down as null rather
        // than as the column's default null value
        metrics[i] = _nullHandlingEnabled && metricReader.isNull(docId) ? null : metricReader.getValue(docId);
      }
    }
    return new Record(dimensions, metrics);
  }

  /// Merges a segment record (raw) into the aggregated record.
  ///
  /// Will create a new aggregated record if the current one is `null`.
  ///
  /// @param aggregatedRecord Aggregated record
  /// @param segmentRecord Segment record
  /// @return Merged record
  Record mergeSegmentRecord(@Nullable Record aggregatedRecord, Record segmentRecord) {
    if (aggregatedRecord == null) {
      int[] dimensions = Arrays.copyOf(segmentRecord._dimensions, _numDimensions);
      Object[] metrics = new Object[_numMetrics];
      for (int i = 0; i < _numMetrics; i++) {
        if (_metricReaders[i] == null) {
          // COUNT(*) has no reader and counts every row
          assert _valueAggregators[i].getAggregationType() == AggregationFunctionType.COUNT;
          metrics[i] = 1L;
          continue;
        }
        // A null raw value only occurs in a null-aware star-tree, where it is excluded from the aggregation. The
        // aggregated value stays null until the group sees its first non-null value.
        Object rawValue = segmentRecord._metrics[i];
        metrics[i] = rawValue != null ? _valueAggregators[i].getInitialAggregatedValue(rawValue) : null;
      }
      return new Record(dimensions, metrics);
    } else {
      for (int i = 0; i < _numMetrics; i++) {
        if (_metricReaders[i] == null) {
          assert _valueAggregators[i].getAggregationType() == AggregationFunctionType.COUNT;
          aggregatedRecord._metrics[i] = ((long) aggregatedRecord._metrics[i]) + 1;
          continue;
        }
        Object rawValue = segmentRecord._metrics[i];
        if (rawValue == null) {
          continue;
        }
        Object aggregatedValue = aggregatedRecord._metrics[i];
        aggregatedRecord._metrics[i] =
            aggregatedValue != null ? _valueAggregators[i].applyRawValue(aggregatedValue, rawValue)
                : _valueAggregators[i].getInitialAggregatedValue(rawValue);
      }
      return aggregatedRecord;
    }
  }

  /// Merges a star-tree record (aggregated) into the aggregated record.
  ///
  /// Will create a new aggregated record if the current one is `null`.
  ///
  /// @param aggregatedRecord Aggregated record
  /// @param starTreeRecord Star-tree record
  /// @return Merged record
  Record mergeStarTreeRecord(@Nullable Record aggregatedRecord, Record starTreeRecord) {
    if (aggregatedRecord == null) {
      int[] dimensions = Arrays.copyOf(starTreeRecord._dimensions, _numDimensions);
      Object[] metrics = new Object[_numMetrics];
      for (int i = 0; i < _numMetrics; i++) {
        // A null value means the group aggregated over no non-null input, which only occurs in a null-aware star-tree
        Object value = starTreeRecord._metrics[i];
        metrics[i] = value != null ? _valueAggregators[i].cloneAggregatedValue(value) : null;
      }
      return new Record(dimensions, metrics);
    } else {
      for (int i = 0; i < _numMetrics; i++) {
        Object value = starTreeRecord._metrics[i];
        if (value == null) {
          continue;
        }
        Object aggregatedValue = aggregatedRecord._metrics[i];
        aggregatedRecord._metrics[i] =
            aggregatedValue != null ? _valueAggregators[i].applyAggregatedValue(aggregatedValue, value)
                : _valueAggregators[i].cloneAggregatedValue(value);
      }
      return aggregatedRecord;
    }
  }

  @Override
  public void build()
      throws Exception {
    long startTime = System.currentTimeMillis();
    LOGGER.info("Starting building star-tree with config: {}", _builderConfig);

    int numSegmentRecords = _segment.getSegmentMetadata().getTotalDocs();
    Iterator<Record> recordIterator = sortAndAggregateSegmentRecords(numSegmentRecords);
    while (recordIterator.hasNext()) {
      appendToStarTree(recordIterator.next());
    }
    int numStarTreeRecords = _numDocs;
    LOGGER.info("Generated {} star-tree records from {} segment records", numStarTreeRecords, numSegmentRecords);

    constructStarTree(_rootNode, 0, _numDocs);
    int numRecordsUnderStarNode = _numDocs - numStarTreeRecords;
    LOGGER.info("Finished constructing star-tree, got {} tree nodes and {} records under star-node", _numNodes,
        numRecordsUnderStarNode);

    createAggregatedDocs(_rootNode);
    int numAggregatedRecords = _numDocs - numStarTreeRecords - numRecordsUnderStarNode;
    LOGGER.info("Finished creating aggregated documents, got {} aggregated records", numAggregatedRecords);

    createForwardIndexes();
    StarTreeBuilderUtils.serializeTree(new File(_outputDir, StarTreeV2Constants.STAR_TREE_INDEX_FILE_NAME), _rootNode,
        _dimensionsSplitOrder, _numNodes);
    _builderConfig.writeMetadata(_metadataProperties, _numDocs);

    LOGGER.info("Finished building star-tree in {}ms", System.currentTimeMillis() - startTime);
  }

  private void appendToStarTree(Record record)
      throws IOException {
    appendRecord(record);
    _numDocs++;
  }

  private TreeNode getNewNode() {
    _numNodes++;
    return new TreeNode();
  }

  private void constructStarTree(TreeNode node, int startDocId, int endDocId)
      throws IOException {
    int childDimensionId = node._dimensionId + 1;
    if (childDimensionId == _numDimensions) {
      return;
    }

    // Construct all non-star children nodes
    node._childDimensionId = childDimensionId;
    Map<Integer, TreeNode> children = constructNonStarNodes(startDocId, endDocId, childDimensionId);
    node._children = children;

    // Construct star-node if required
    if (!_skipStarNodeCreationForDimensions.contains(childDimensionId) && children.size() > 1) {
      children.put(StarTreeNode.ALL, constructStarNode(startDocId, endDocId, childDimensionId));
    }

    // Further split on child nodes if required
    for (TreeNode child : children.values()) {
      if (child._endDocId - child._startDocId > _maxLeafRecords) {
        constructStarTree(child, child._startDocId, child._endDocId);
      }
    }
  }

  private Map<Integer, TreeNode> constructNonStarNodes(int startDocId, int endDocId, int dimensionId)
      throws IOException {
    Map<Integer, TreeNode> nodes = new HashMap<>();
    int nodeStartDocId = startDocId;
    int nodeDimensionValue = getDimensionValue(startDocId, dimensionId);
    for (int i = startDocId + 1; i < endDocId; i++) {
      int dimensionValue = getDimensionValue(i, dimensionId);
      if (dimensionValue != nodeDimensionValue) {
        TreeNode child = getNewNode();
        child._dimensionId = dimensionId;
        child._dimensionValue = nodeDimensionValue;
        child._startDocId = nodeStartDocId;
        child._endDocId = i;
        nodes.put(nodeDimensionValue, child);

        nodeStartDocId = i;
        nodeDimensionValue = dimensionValue;
      }
    }
    TreeNode lastNode = getNewNode();
    lastNode._dimensionId = dimensionId;
    lastNode._dimensionValue = nodeDimensionValue;
    lastNode._startDocId = nodeStartDocId;
    lastNode._endDocId = endDocId;
    nodes.put(nodeDimensionValue, lastNode);
    return nodes;
  }

  private TreeNode constructStarNode(int startDocId, int endDocId, int dimensionId)
      throws IOException {
    TreeNode starNode = getNewNode();
    starNode._dimensionId = dimensionId;
    starNode._dimensionValue = StarTreeNode.ALL;
    starNode._startDocId = _numDocs;
    Iterator<Record> recordIterator = generateRecordsForStarNode(startDocId, endDocId, dimensionId);
    while (recordIterator.hasNext()) {
      appendToStarTree(recordIterator.next());
    }
    starNode._endDocId = _numDocs;
    return starNode;
  }

  private Record createAggregatedDocs(TreeNode node)
      throws IOException {
    Record aggregatedRecord = null;
    if (node._children == null) {
      // For leaf node

      if (node._startDocId == node._endDocId - 1) {
        // If it has only one document, use it as the aggregated document
        aggregatedRecord = getStarTreeRecord(node._startDocId);
        node._aggregatedDocId = node._startDocId;
      } else {
        // If it has multiple documents, aggregate all of them
        for (int i = node._startDocId; i < node._endDocId; i++) {
          aggregatedRecord = mergeStarTreeRecord(aggregatedRecord, getStarTreeRecord(i));
        }
        assert aggregatedRecord != null;
        for (int i = node._dimensionId + 1; i < _numDimensions; i++) {
          aggregatedRecord._dimensions[i] = StarTreeV2Constants.STAR_IN_FORWARD_INDEX;
        }
        node._aggregatedDocId = _numDocs;
        appendToStarTree(aggregatedRecord);
      }
    } else {
      // For non-leaf node

      if (node._children.containsKey(StarTreeNode.ALL)) {
        // If it has star child, use the star child aggregated document directly
        for (TreeNode child : node._children.values()) {
          if (child._dimensionValue == StarTreeNode.ALL) {
            aggregatedRecord = createAggregatedDocs(child);
            node._aggregatedDocId = child._aggregatedDocId;
          } else {
            createAggregatedDocs(child);
          }
        }
      } else {
        // If no star child exists, aggregate all aggregated documents from non-star children
        for (TreeNode child : node._children.values()) {
          aggregatedRecord = mergeStarTreeRecord(aggregatedRecord, createAggregatedDocs(child));
        }
        assert aggregatedRecord != null;
        for (int i = node._dimensionId + 1; i < _numDimensions; i++) {
          aggregatedRecord._dimensions[i] = StarTreeV2Constants.STAR_IN_FORWARD_INDEX;
        }
        node._aggregatedDocId = _numDocs;
        appendToStarTree(aggregatedRecord);
      }
    }
    return aggregatedRecord;
  }

  private void createForwardIndexes()
      throws Exception {
    SingleValueUnsortedForwardIndexCreator[] dimensionIndexCreators =
        new SingleValueUnsortedForwardIndexCreator[_numDimensions];
    for (int i = 0; i < _numDimensions; i++) {
      // A null-aware star-tree reserves one extra dictionary id per dimension for null values, which may widen the
      // forward index by one bit per value. StarTreeLoaderUtils applies the same adjustment when reading it back.
      int numValues = _nullHandlingEnabled ? _dimensionCardinalities[i] + 1 : _dimensionCardinalities[i];
      dimensionIndexCreators[i] =
          new SingleValueUnsortedForwardIndexCreator(_outputDir, _dimensionsSplitOrder[i], numValues, _numDocs);
    }

    // Null vectors are only created for a null-aware star-tree. Dimensions need one as well as metrics: the reserved
    // null dictionary id is out of range for the segment dictionary the star-tree shares, so the query side relies on
    // the null vector rather than on the stored dictionary id.
    NullValueVectorCreator[] dimensionNullValueVectorCreators =
        _nullHandlingEnabled ? new NullValueVectorCreator[_numDimensions] : null;
    NullValueVectorCreator[] metricNullValueVectorCreators =
        _nullHandlingEnabled ? new NullValueVectorCreator[_numMetrics] : null;
    if (_nullHandlingEnabled) {
      for (int i = 0; i < _numDimensions; i++) {
        dimensionNullValueVectorCreators[i] = new NullValueVectorCreator(_outputDir, _dimensionsSplitOrder[i]);
      }
      for (int i = 0; i < _numMetrics; i++) {
        metricNullValueVectorCreators[i] = new NullValueVectorCreator(_outputDir, _metrics[i]);
      }
    }

    ForwardIndexCreator[] metricIndexCreators = new ForwardIndexCreator[_numMetrics];
    for (int i = 0; i < _numMetrics; i++) {
      String metric = _metrics[i];
      ValueAggregator valueAggregator = _valueAggregators[i];
      DataType valueType = valueAggregator.getAggregatedValueType();
      AggregationSpec aggregationSpec = _aggregationSpecs[i];
      ChunkCompressionType compressionType = ChunkCompressionType.valueOf(aggregationSpec.getCompressionCodec().name());
      if (valueType == BYTES) {
        metricIndexCreators[i] =
            new SingleValueVarByteRawIndexCreator(_outputDir, compressionType, metric, _numDocs, BYTES,
                valueAggregator.getMaxAggregatedValueByteSize(), aggregationSpec.isDeriveNumDocsPerChunk(),
                aggregationSpec.getIndexVersion(), aggregationSpec.getTargetMaxChunkSizeBytes(),
                aggregationSpec.getTargetDocsPerChunk());
      } else {
        metricIndexCreators[i] =
            new SingleValueFixedByteRawIndexCreator(_outputDir, compressionType, metric, _numDocs, valueType,
                aggregationSpec.getIndexVersion(), aggregationSpec.getTargetDocsPerChunk());
      }
    }

    Exception t = null;
    try {
      for (int docId = 0; docId < _numDocs; docId++) {
        Record record = getStarTreeRecord(docId);
        for (int i = 0; i < _numDimensions; i++) {
          int dictId = record._dimensions[i];
          dimensionIndexCreators[i].putDictId(dictId);
          // Star records store STAR_IN_FORWARD_INDEX (0) for the starred dimension, which never collides with the
          // reserved null dictionary id, so star records are correctly left out of the null vector
          if (dimensionNullValueVectorCreators != null && dictId == _dimensionCardinalities[i]) {
            dimensionNullValueVectorCreators[i].setNull(docId);
          }
        }
        for (int i = 0; i < _numMetrics; i++) {
          ValueAggregator valueAggregator = _valueAggregators[i];
          Object value = record._metrics[i];
          if (value == null) {
            // The group aggregated over no non-null input. Only COUNT still has a well-defined result of its own
            // (0); every other aggregator answers SQL NULL and gets a placeholder in the forward index masked by
            // the null vector.
            assert _nullHandlingEnabled;
            value = valueAggregator.getAllNullAggregatedValue();
            if (value == null) {
              metricNullValueVectorCreators[i].setNull(docId);
            }
          }
          putMetricValue(metricIndexCreators[i], valueAggregator, value);
        }
      }
      if (_nullHandlingEnabled) {
        for (NullValueVectorCreator nullValueVectorCreator : dimensionNullValueVectorCreators) {
          nullValueVectorCreator.seal();
        }
        for (NullValueVectorCreator nullValueVectorCreator : metricNullValueVectorCreators) {
          nullValueVectorCreator.seal();
        }
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while creating forward indexes", e);
      t = e;
    } finally {
      for (int i = 0; i < dimensionIndexCreators.length; i++) {
        try {
          dimensionIndexCreators[i].close();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while closing dimension index creator for dimension: {}",
              _dimensionsSplitOrder[i], e);
          if (t == null) {
            t = e;
          }
        }
      }
      for (int i = 0; i < metricIndexCreators.length; i++) {
        try {
          metricIndexCreators[i].close();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while closing metric index creator for metric: {}", _metrics[i], e);
          if (t == null) {
            t = e;
          }
        }
      }
    }
    if (t != null) {
      throw t;
    }
  }

  /// Writes an aggregated metric value into the forward index.
  ///
  /// A `null` value is a group that aggregates to SQL `NULL`; it is recorded in the metric's null vector and stored
  /// here as the aggregated type's zero value, which the query side never reads.
  private static void putMetricValue(ForwardIndexCreator metricIndexCreator, ValueAggregator valueAggregator,
      @Nullable Object value)
      throws IOException {
    switch (valueAggregator.getAggregatedValueType()) {
      case INT:
        metricIndexCreator.putInt(value != null ? (int) value : 0);
        break;
      case LONG:
        metricIndexCreator.putLong(value != null ? (long) value : 0L);
        break;
      case FLOAT:
        metricIndexCreator.putFloat(value != null ? (float) value : 0f);
        break;
      case DOUBLE:
        metricIndexCreator.putDouble(value != null ? (double) value : 0d);
        break;
      case BYTES:
        metricIndexCreator.putBytes(
            value != null ? valueAggregator.serializeAggregatedValue(value) : ArrayUtils.EMPTY_BYTE_ARRAY);
        break;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public void close()
      throws IOException {
    for (PinotSegmentColumnReader dimensionReader : _dimensionReaders) {
      dimensionReader.close();
    }
    for (PinotSegmentColumnReader metricReader : _metricReaders) {
      if (metricReader != null) {
        metricReader.close();
      }
    }
  }
}
