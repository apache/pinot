/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


/**
 * Uses file to build the star tree. Each row is divided into dimension and metrics. Time is added to dimension list.
 * We use the split order to build the tree. In most cases, split order will be ranked depending on the cardinality (descending order).
 * Time column will be excluded or last entry in split order irrespective of its cardinality
 * This is a recursive algorithm where we branch on one dimension at every level.
 *
 * <b>Psuedo algo</b>
 * <code>
 *
 * build(){
 *  let table(1,N) consists of N input rows
 *  table.sort(1,N) //sort the table on all dimensions, according to split order
 *  constructTree(table, 0, N, 0);
 * }
 * constructTree(table,start,end, level){
 *    splitDimensionName = dimensionsSplitOrder[level]
 *    groupByResult<dimName, length> = table.groupBy(dimensionsSplitOrder[level]); //returns the number of rows for each value in splitDimension
 *    int rangeStart = 0;
 *    for each ( entry<dimName,length> groupByResult){
 *      if(entry.length > minThreshold){
 *        constructTree(table, rangeStart, rangeStart + entry.length, level +1);
 *      }
 *      rangeStart = rangeStart + entry.length;
 *      updateStarTree() //add new child
 *    }
 *
 *    //create a star tree node
 *
 *    aggregatedRows = table.uniqueAfterRemovingAttributeAndAggregateMetrics(start,end, splitDimensionName);
 *    for(each row in aggregatedRows_
 *    table.add(row);
 *    if(aggregateRows.size > minThreshold) {
 *      table.sort(end, end + aggregatedRows.size);
 *      constructStarTree(table, end, end + aggregatedRows.size, level +1);
 *    }
 * }
 * </code>
 */
public class OffHeapStarTreeBuilder implements StarTreeBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(OffHeapStarTreeBuilder.class);
  File dataFile;
  private DataOutputStream dataBuffer;
  int rawRecordCount = 0;
  int aggRecordCount = 0;
  private List<String> dimensionsSplitOrder;
  private Set<String> skipStarNodeCreationForDimensions;
  private Set<String> skipMaterializationForDimensions;

  private int maxLeafRecords;
  private StarTree starTree;
  private StarTreeIndexNode starTreeRootIndexNode;
  private int numDimensions;
  private int numMetrics;
  private List<String> dimensionNames;
  private List<String> metricNames;
  private String timeColumnName;
  private List<DataType> dimensionTypes;
  private List<DataType> metricTypes;
  private Map<String, Object> dimensionNameToStarValueMap;
  private HashBiMap<String, Integer> dimensionNameToIndexMap;
  private Map<String, Integer> metricNameToIndexMap;
  private int dimensionSizeBytes;
  private int metricSizeBytes;
  private File outDir;
  private Map<String, HashBiMap<Object, Integer>> dictionaryMap;

  boolean debugMode = false;
  private int[] sortOrder;
  private int skipMaterializationCardinalityThreshold;

  public void init(StarTreeBuilderConfig builderConfig) throws Exception {
    Schema schema = builderConfig.schema;
    timeColumnName = schema.getTimeColumnName();
    this.dimensionsSplitOrder = builderConfig.dimensionsSplitOrder;
    skipStarNodeCreationForDimensions = builderConfig.getSkipStarNodeCreationForDimensions();
    skipMaterializationForDimensions = builderConfig.getSkipMaterializationForDimensions();
    skipMaterializationCardinalityThreshold = builderConfig.getSkipMaterializationCardinalityThreshold();

    this.maxLeafRecords = builderConfig.maxLeafRecords;
    this.outDir = builderConfig.getOutDir();
    if (outDir == null) {
      outDir = new File(System.getProperty("java.io.tmpdir"), V1Constants.STAR_TREE_INDEX_DIR + "_" + DateTime.now());
    }
    LOG.debug("Index output directory:{}", outDir);

    dimensionTypes = new ArrayList<>();
    dimensionNames = new ArrayList<>();
    dimensionNameToIndexMap = HashBiMap.create();
    dimensionNameToStarValueMap = new HashMap<>();
    dictionaryMap = new HashMap<>();

    //READ DIMENSIONS COLUMNS
    List<DimensionFieldSpec> dimensionFieldSpecs = schema.getDimensionFieldSpecs();
    for (int index = 0; index < dimensionFieldSpecs.size(); index++) {
      DimensionFieldSpec spec = dimensionFieldSpecs.get(index);
      String dimensionName = spec.getName();
      dimensionNames.add(dimensionName);
      dimensionNameToIndexMap.put(dimensionName, index);
      Object starValue;
      starValue = getAllStarValue(spec);
      dimensionNameToStarValueMap.put(dimensionName, starValue);
      dimensionTypes.add(spec.getDataType());
      HashBiMap<Object, Integer> dictionary = HashBiMap.create();
      dictionaryMap.put(dimensionName, dictionary);
    }
    //treat time column as just another dimension, only difference is that we will never split on this dimension unless explicitly specified in split order
    if (timeColumnName != null) {
      dimensionNames.add(timeColumnName);
      dimensionTypes.add(schema.getTimeFieldSpec().getDataType());
      int index = dimensionNameToIndexMap.size();
      dimensionNameToIndexMap.put(timeColumnName, index);
      HashBiMap<Object, Integer> dictionary = HashBiMap.create();
      dictionaryMap.put(schema.getTimeColumnName(), dictionary);
    }
    dimensionSizeBytes = dimensionNames.size() * Integer.SIZE / 8;
    this.numDimensions = dimensionNames.size();

    //READ METRIC COLUMNS
    this.metricTypes = new ArrayList<>();
    this.metricNames = new ArrayList<>();

    this.metricNameToIndexMap = new HashMap<>();
    this.metricSizeBytes = 0;
    List<MetricFieldSpec> metricFieldSpecs = schema.getMetricFieldSpecs();
    for (int index = 0; index < metricFieldSpecs.size(); index++) {
      MetricFieldSpec spec = metricFieldSpecs.get(index);
      String metricName = spec.getName();
      metricNames.add(metricName);
      metricNameToIndexMap.put(metricName, index);
      DataType dataType = spec.getDataType();
      metricTypes.add(dataType);
      metricSizeBytes += dataType.size();
    }
    this.numMetrics = metricNames.size();
    builderConfig.getOutDir().mkdirs();
    dataFile = new File(outDir, "star-tree.buf");
    dataBuffer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile)));

    //INITIALIZE THE ROOT NODE
    this.starTreeRootIndexNode = new StarTreeIndexNode();
    this.starTreeRootIndexNode.setDimensionName(StarTreeIndexNode.all());
    this.starTreeRootIndexNode.setDimensionValue(StarTreeIndexNode.all());
    this.starTreeRootIndexNode.setLevel(0);
    LOG.debug("dimensionNames:{}", dimensionNames);
    LOG.debug("metricNames:{}", metricNames);
  }

  /**
   * Validate the split order by removing any dimensions that may be part of the skip materialization list.
   * @param dimensionsSplitOrder
   * @param skipMaterializationForDimensions
   * @return
   */
  private List<String> sanitizeSplitOrder(List<String> dimensionsSplitOrder,
      Set<String> skipMaterializationForDimensions) {
    List<String> validatedSplitOrder = new ArrayList<String>();
    for (String dimension : dimensionsSplitOrder) {
      if (skipMaterializationForDimensions == null || !skipMaterializationForDimensions.contains(dimension)) {
        LOG.info("Adding dimension {} to split order", dimension);
        validatedSplitOrder.add(dimension);
      } else {
        LOG.info(
            "Dimension {} cannot be part of 'dimensionSplitOrder' and 'skipMaterializationForDimensions', removing it from split order",
            dimension);
      }
    }
    return validatedSplitOrder;
  }

  private Object getAllStarValue(DimensionFieldSpec spec) throws Exception {
    switch (spec.getDataType()) {
      case STRING:
        return "ALL";
      case BOOLEAN:
      case BYTE:
      case CHAR:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
        return spec.getDefaultNullValue();
      case OBJECT:
      case SHORT:
      case DOUBLE_ARRAY:
      case CHAR_ARRAY:
      case FLOAT_ARRAY:
      case INT_ARRAY:
      case LONG_ARRAY:
      case SHORT_ARRAY:
      case STRING_ARRAY:
      case BYTE_ARRAY:
      default:
        throw new Exception("Unsupported dimension data type" + spec);
    }
  }

  public GenericRow toGenericRow(DimensionBuffer dimensionKey, MetricBuffer metricsHolder) {
    GenericRow row = new GenericRow();
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < dimensionNames.size(); i++) {
      String dimName = dimensionNames.get(i);
      BiMap<Integer, Object> inverseDictionary = dictionaryMap.get(dimName).inverse();
      Object dimValue = inverseDictionary.get(dimensionKey.getDimension(i));
      if (dimValue == null) {
        dimValue = dimensionNameToStarValueMap.get(dimName);
      }
      map.put(dimName, dimValue);
    }
    for (int i = 0; i < numMetrics; i++) {
      String metName = metricNames.get(i);
      map.put(metName, metricsHolder.get(i));
    }
    row.init(map);
    return row;
  }

  public void append(GenericRow row) throws Exception {
    DimensionBuffer dimension = new DimensionBuffer(numDimensions);
    for (int i = 0; i < dimensionNames.size(); i++) {
      String dimName = dimensionNames.get(i);
      Map<Object, Integer> dictionary = dictionaryMap.get(dimName);
      Object dimValue = row.getValue(dimName);
      if (dimValue == null) {
        //TODO: Have another default value to represent STAR. Using default value to represent STAR as of now.
        //It does not matter during query execution, since we know that values is STAR from the star tree
        dimValue = dimensionNameToStarValueMap.get(dimName);
      }
      if (!dictionary.containsKey(dimValue)) {
        dictionary.put(dimValue, dictionary.size());
      }
      dimension.setDimension(i, dictionary.get(dimValue));
    }
    Number[] numbers = new Number[numMetrics];
    for (int i = 0; i < numMetrics; i++) {
      String metName = metricNames.get(i);
      numbers[i] = (Number) row.getValue(metName);
    }
    MetricBuffer metrics = new MetricBuffer(numbers);
    append(dimension, metrics);
  }

  public void append(DimensionBuffer dimension, MetricBuffer metrics) throws Exception {
    appendToRawBuffer(dimension, metrics);
  }

  private void appendToRawBuffer(DimensionBuffer dimension, MetricBuffer metrics) throws IOException {
    appendToBuffer(dataBuffer, dimension, metrics);
    rawRecordCount++;
  }

  private void appendToAggBuffer(DimensionBuffer dimension, MetricBuffer metrics) throws IOException {
    appendToBuffer(dataBuffer, dimension, metrics);
    aggRecordCount++;
  }

  private void appendToBuffer(DataOutputStream dos, DimensionBuffer dimensions, MetricBuffer metricHolder)
      throws IOException {
    for (int i = 0; i < numDimensions; i++) {
      dos.writeInt(dimensions.getDimension(i));
    }
    dos.write(metricHolder.toBytes(metricSizeBytes, metricTypes));

  }

  public void build() throws Exception {
    if (skipMaterializationForDimensions == null || skipMaterializationForDimensions.isEmpty()) {
      skipMaterializationForDimensions = computeDefaultDimensionsToSkipMaterialization();
    }
    if (dimensionsSplitOrder == null || dimensionsSplitOrder.isEmpty()) {
      dimensionsSplitOrder = computeDefaultSplitOrder();
    }

    // Remove any dimensions from split order that would be not be materialized.
    dimensionsSplitOrder = sanitizeSplitOrder(dimensionsSplitOrder, skipMaterializationForDimensions);

    LOG.debug("Split order:{}", dimensionsSplitOrder);
    long start = System.currentTimeMillis();
    dataBuffer.flush();
    sort(dataFile, 0, rawRecordCount);
    constructStarTree(starTreeRootIndexNode, 0, rawRecordCount, 0, dataFile);
    long end = System.currentTimeMillis();
    LOG.debug("Took {} ms to build star tree index. Original records:{} Materialized record:{}", (end - start),
        rawRecordCount, aggRecordCount);
    starTree = new StarTree(starTreeRootIndexNode, dimensionNameToIndexMap);
    File treeBinary = new File(outDir, "star-tree.bin");
    LOG.debug("Saving tree binary at: {} ", treeBinary);
    starTree.writeTree(new BufferedOutputStream(new FileOutputStream(treeBinary)));
    printTree(starTreeRootIndexNode, 0);
    LOG.debug("Finished build tree. out dir: {} ", outDir);
    dataBuffer.close();
  }

  private void printTree(StarTreeIndexNode node, int level) {
    for (int i = 0; i < level; i++) {
      LOG.debug("  ");
    }
    BiMap<Integer, String> inverse = dimensionNameToIndexMap.inverse();
    String dimName = "ALL";
    Object dimValue = "ALL";
    if (node.getDimensionName() != StarTreeIndexNode.all()) {
      dimName = inverse.get(node.getDimensionName());
    }
    if (node.getDimensionValue() != StarTreeIndexNode.all()) {
      dimValue = dictionaryMap.get(dimName).inverse().get(node.getDimensionValue());
    }
    String formattedOutput =
        Objects.toStringHelper(node).add("nodeId", node.getNodeId()).add("level", level).add("dimensionName", dimName)
            .add("dimensionValue", dimValue).add("childDimensionName", inverse.get(node.getChildDimensionName()))
            .add("childCount", node.getChildren() == null ? 0 : node.getChildren().size())
            .add("startDocumentId", node.getStartDocumentId()).add("endDocumentId", node.getEndDocumentId())
            .add("documentCount", (node.getEndDocumentId() - node.getStartDocumentId())).toString();
    LOG.debug(formattedOutput);

    if (!node.isLeaf()) {
      for (StarTreeIndexNode child : node.getChildren().values()) {
        printTree(child, level + 1);
      }
    }

  }

  private List<String> computeDefaultSplitOrder() {
    ArrayList<String> defaultSplitOrder = new ArrayList<>();
    //include only the dimensions not time column. Also, assumes that skipMaterializationForDimensions is built.
    for (String dimensionName : dimensionNames) {
      if (skipMaterializationForDimensions != null && !skipMaterializationForDimensions.contains(dimensionName)) {
        defaultSplitOrder.add(dimensionName);
      }
    }
    if (timeColumnName != null) {
      defaultSplitOrder.remove(timeColumnName);
    }
    Collections.sort(defaultSplitOrder, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return dictionaryMap.get(o2).size() - dictionaryMap.get(o1).size(); //descending
      }
    });
    return defaultSplitOrder;
  }


  private Set<String> computeDefaultDimensionsToSkipMaterialization() {
    Set<String> skipDimensions = new HashSet<String>();
    for (String dimensionName : dimensionNames) {
      if (dictionaryMap.get(dimensionName).size() > skipMaterializationCardinalityThreshold) {
        skipDimensions.add(dimensionName);
      }
    }
    return skipDimensions;
  }


  /*
   * Sorts the file on all dimensions
   */
  private void sort(File file, int startDocId, int endDocId) throws IOException {
    if (debugMode) {
      LOG.info("BEFORE SORTING");
      printFile(file, startDocId, endDocId);
    }

    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes, getSortOrder());
    dataSorter.sort(startDocId, endDocId, 0, dimensionSizeBytes);
    if (debugMode) {
      LOG.info("AFTER SORTING");
      printFile(file, startDocId, endDocId);
    }
  }

  private int[] getSortOrder() {
    if (sortOrder == null) {
      sortOrder = new int[dimensionNames.size()];
      for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
        sortOrder[i] = dimensionNameToIndexMap.get(dimensionsSplitOrder.get(i));
      }
      //add remaining dimensions that were not part of dimensionsSplitOrder
      int counter = 0;
      for (String dimName : dimensionNames) {
        if (!dimensionsSplitOrder.contains(dimName)) {
          sortOrder[dimensionsSplitOrder.size() + counter] = dimensionNameToIndexMap.get(dimName);
          counter = counter + 1;
        }
      }
    }
    return sortOrder;
  }

  private void printFile(File file, int startDocId, int endDocId) throws IOException {
    LOG.info("Contents of file:{} from:{} to:{}", file.getName(), startDocId, endDocId);
    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes, getSortOrder());
    Iterator<Pair<byte[], byte[]>> iterator = dataSorter.iterator(startDocId, endDocId);
    int numRecordsToPrint = 100;
    int counter = 0;
    while (iterator.hasNext()) {
      Pair<byte[], byte[]> next = iterator.next();
      LOG.info("{}, {}", DimensionBuffer.fromBytes(next.getLeft()),
          MetricBuffer.fromBytes(next.getRight(), metricTypes));
      if (counter++ == numRecordsToPrint) {
        break;
      }
    }
  }

  private int constructStarTree(StarTreeIndexNode node, int startDocId, int endDocId, int level, File file)
      throws Exception {
    //node.setStartDocumentId(startDocId);
    int docsAdded = 0;
    if (level == dimensionsSplitOrder.size() - 1) {
      return 0;
    }
    String splitDimensionName = dimensionsSplitOrder.get(level);
    Integer splitDimensionId = dimensionNameToIndexMap.get(splitDimensionName);
    LOG.debug("Building tree at level:{} using file:{} from startDoc:{} endDocId:{} splitting on dimension:{}", level,
        file.getName(), startDocId, endDocId, splitDimensionName);
    Map<Integer, IntPair> sortGroupBy = groupBy(startDocId, endDocId, splitDimensionId, file);
    LOG.debug("Group stats:{}", sortGroupBy);
    node.setChildDimensionName(splitDimensionId);
    node.setChildren(new HashMap<Integer, StarTreeIndexNode>());
    for (int childDimensionValue : sortGroupBy.keySet()) {
      StarTreeIndexNode child = new StarTreeIndexNode();
      child.setDimensionName(splitDimensionId);
      child.setDimensionValue(childDimensionValue);
      child.setParent(node);
      child.setLevel(node.getLevel() + 1);

      // n.b. We will number the nodes later using BFS after fully split

      // Add child to parent
      node.getChildren().put(childDimensionValue, child);

      int childDocs = 0;
      IntPair range = sortGroupBy.get(childDimensionValue);
      if (range.getRight() - range.getLeft() > maxLeafRecords) {
        childDocs = constructStarTree(child, range.getLeft(), range.getRight(), level + 1, file);
        docsAdded += childDocs;
      }

      // Either range <= maxLeafRecords, or we did not split further (last level).
      if (childDocs == 0) {
        child.setStartDocumentId(range.getLeft());
        child.setEndDocumentId(range.getRight());
      }
    }

    // Return if star node does not need to be created.
    if (skipStarNodeCreationForDimensions != null && skipStarNodeCreationForDimensions.contains(splitDimensionName)) {
      return docsAdded;
    }

    //create star node
    StarTreeIndexNode starChild = new StarTreeIndexNode();
    starChild.setDimensionName(splitDimensionId);
    starChild.setDimensionValue(StarTreeIndexNode.all());
    starChild.setParent(node);
    starChild.setLevel(node.getLevel() + 1);
    // n.b. We will number the nodes later using BFS after fully split

    // Add child to parent
    node.getChildren().put(StarTreeIndexNode.all(), starChild);

    Iterator<Pair<DimensionBuffer, MetricBuffer>> iterator =
        uniqueCombinations(startDocId, endDocId, file, splitDimensionId);
    int rowsAdded = 0;
    int startOffset = rawRecordCount + aggRecordCount;
    while (iterator.hasNext()) {
      Pair<DimensionBuffer, MetricBuffer> next = iterator.next();
      DimensionBuffer dimension = next.getLeft();
      MetricBuffer metricsHolder = next.getRight();
      LOG.debug("Adding row:{}", dimension);
      appendToAggBuffer(dimension, metricsHolder);
      rowsAdded++;
    }
    docsAdded += rowsAdded;
    LOG.debug("Added {} additional records at level {}", rowsAdded, level);
    //flush
    dataBuffer.flush();

    int childDocs = 0;
    if (rowsAdded >= maxLeafRecords) {
      sort(dataFile, startOffset, startOffset + rowsAdded);
      childDocs = constructStarTree(starChild, startOffset, startOffset + rowsAdded, level + 1, dataFile);
      docsAdded += childDocs;
    }

    // Either rowsAdded < maxLeafRecords, or we did not split further (last level).
    if (childDocs == 0) {
      starChild.setStartDocumentId(startOffset);
      starChild.setEndDocumentId(startOffset + rowsAdded);
    }
    //node.setEndDocumentId(endDocId + docsAdded);
    return docsAdded;
  }

  /**
   * Assumes the file is already sorted, returns the unique combinations after removing a specified dimension.
   * Aggregates the metrics for each unique combination, currently only sum is supported by default
   * @param startDocId
   * @param endDocId
   * @param file
   * @param splitDimensionId
   * @return
   * @throws Exception
   */
  private Iterator<Pair<DimensionBuffer, MetricBuffer>> uniqueCombinations(int startDocId, int endDocId, File file,
      int splitDimensionId) throws Exception {
    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes, getSortOrder());
    Iterator<Pair<byte[], byte[]>> iterator1 = dataSorter.iterator(startDocId, endDocId);
    File tempFile = new File(outDir, file.getName() + "_" + startDocId + "_" + endDocId + ".unique.tmp");
    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)));
    while (iterator1.hasNext()) {
      Pair<byte[], byte[]> next = iterator1.next();
      byte[] dimensionBuffer = next.getLeft();
      byte[] metricBuffer = next.getRight();
      DimensionBuffer dimensions = DimensionBuffer.fromBytes(dimensionBuffer);
      for (int i = 0; i < numDimensions; i++) {
        String dimensionName = dimensionNameToIndexMap.inverse().get(i);
        if (i == splitDimensionId || (skipMaterializationForDimensions != null &&
            skipMaterializationForDimensions.contains(dimensionName))) {
          dos.writeInt(StarTreeIndexNode.all());
        } else {
          dos.writeInt(dimensions.getDimension(i));
        }
      }
      dos.write(metricBuffer);
    }
    dos.close();
    dataSorter = new StarTreeDataTable(tempFile, dimensionSizeBytes, metricSizeBytes, getSortOrder());
    dataSorter.sort(0, endDocId - startDocId);
    if (debugMode) {
      printFile(tempFile, 0, endDocId - startDocId);
    }
    final Iterator<Pair<byte[], byte[]>> iterator = dataSorter.iterator(0, endDocId - startDocId);
    return new Iterator<Pair<DimensionBuffer, MetricBuffer>>() {

      Pair<DimensionBuffer, MetricBuffer> prev = null;
      boolean done = false;

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasNext() {
        return !done;
      }

      @Override
      public Pair<DimensionBuffer, MetricBuffer> next() {
        while (iterator.hasNext()) {
          Pair<byte[], byte[]> next = iterator.next();
          byte[] dimBuffer = next.getLeft();
          byte[] metricBuffer = next.getRight();
          if (prev == null) {
            prev = Pair.of(DimensionBuffer.fromBytes(dimBuffer), MetricBuffer.fromBytes(metricBuffer, metricTypes));
          } else {
            Pair<DimensionBuffer, MetricBuffer> current =
                Pair.of(DimensionBuffer.fromBytes(dimBuffer), MetricBuffer.fromBytes(metricBuffer, metricTypes));
            if (!current.getLeft().equals(prev.getLeft())) {
              Pair<DimensionBuffer, MetricBuffer> ret = prev;
              prev = current;
              LOG.debug("Returning unique {}", prev.getLeft());
              return ret;
            } else {
              prev.getRight().aggregate(current.getRight(), metricTypes);
            }
          }
        }
        done = true;
        LOG.debug("Returning unique {}", prev.getLeft());
        return prev;
      }
    };
  }

  /**
   * sorts the file from start to end on a dimension index
   * @param startDocId
   * @param endDocId
   * @param dimension
   * @param file
   * @return
   */
  private Map<Integer, IntPair> groupBy(int startDocId, int endDocId, Integer dimension, File file) {
    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes, getSortOrder());
    return dataSorter.groupByIntColumnCount(startDocId, endDocId, dimension);
  }

  /**
   * Iterator to iterate over the records from startDocId to endDocId
   */
  @Override
  public Iterator<GenericRow> iterator(final int startDocId, final int endDocId) throws Exception {
    StarTreeDataTable dataSorter = new StarTreeDataTable(dataFile, dimensionSizeBytes, metricSizeBytes, getSortOrder());
    final Iterator<Pair<byte[], byte[]>> iterator = dataSorter.iterator(startDocId, endDocId);
    return new Iterator<GenericRow>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public GenericRow next() {
        Pair<byte[], byte[]> pair = iterator.next();
        DimensionBuffer dimensionKey = DimensionBuffer.fromBytes(pair.getLeft());
        MetricBuffer metricsHolder = MetricBuffer.fromBytes(pair.getRight(), metricTypes);
        return toGenericRow(dimensionKey, metricsHolder);
      }
    };
  }

  public JSONObject getStarTreeAsJSON() throws Exception {
    JSONObject json = new JSONObject();
    toJson(json, starTreeRootIndexNode, dictionaryMap);
    return json;
  }

  private void toJson(JSONObject json, StarTreeIndexNode node, Map<String, HashBiMap<Object, Integer>> dictionaryMap)
      throws Exception {
    String dimName = "ALL";
    Object dimValue = "ALL";
    if (node.getDimensionName() != StarTreeIndexNode.all()) {
      dimName = dimensionNames.get(node.getDimensionName());
    }
    if (node.getDimensionValue() != StarTreeIndexNode.all()) {
      dimValue = dictionaryMap.get(dimName).inverse().get(node.getDimensionValue());
    }
    json.put("title", dimName + ":" + dimValue);
    if (node.getChildren() != null) {
      JSONObject[] childJsons = new JSONObject[node.getChildren().size()];
      int index = 0;
      for (Integer child : node.getChildren().keySet()) {
        StarTreeIndexNode childNode = node.getChildren().get(child);
        JSONObject childJson = new JSONObject();
        toJson(childJson, childNode, dictionaryMap);
        childJsons[index++] = childJson;
      }
      json.put("nodes", childJsons);
    }
  }

  @Override
  public void cleanup() {
    if (outDir != null) {
      FileUtils.deleteQuietly(outDir);
    }
  }

  @Override
  public StarTree getTree() {
    return starTree;
  }

  @Override
  public int getTotalRawDocumentCount() {
    return rawRecordCount;
  }

  @Override
  public int getTotalAggregateDocumentCount() {
    return aggRecordCount;
  }

  @Override
  public int getMaxLeafRecords() {
    return maxLeafRecords;
  }

  @Override
  public List<String> getDimensionsSplitOrder() {
    return dimensionsSplitOrder;
  }

  public Map<String, HashBiMap<Object, Integer>> getDictionaryMap() {
    return dictionaryMap;
  }

  public HashBiMap<String, Integer> getDimensionNameToIndexMap() {
    return dimensionNameToIndexMap;
  }

  @Override
  public Set<String> getSkipMaterializationForDimensions() {
    return skipMaterializationForDimensions;
  }
}
