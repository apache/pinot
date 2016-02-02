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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
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
 *    splitDimensionName = splitOrder[level]
 *    groupByResult<dimName, length> = table.groupBy(splitOrder[level]); //returns the number of rows for each value in splitDimension
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
  private List<String> splitOrder;
  private int maxLeafRecords;
  private StarTreeIndexNode starTree;
  private int numDimensions;
  private int numMetrics;
  private List<DataType> dimensionTypes;
  private List<DataType> metricTypes;
  private Map<String, Integer> dimensionNameToIndexMap;
  private Map<String, Integer> metricNameToIndexMap;
  private Schema schema;
  private int dimensionSizeBytes;
  private int metricSizeBytes;
  private File outDir;
  private Map<String, HashBiMap<Object, Integer>> dictionaryMap;
  boolean debugMode = false;

  public void init(StarTreeBuilderConfig builderConfig) throws Exception {
    schema = builderConfig.schema;
    this.splitOrder = builderConfig.splitOrder;
    this.maxLeafRecords = builderConfig.maxLeafRecords;
    this.outDir = builderConfig.getOutDir();
    if (outDir == null) {
      outDir = new File(System.getProperty("java.io.tmpdir"), V1Constants.STAR_TREE_INDEX_DIR + "_" + DateTime.now());
    }
    LOG.info("Index output directory:{}", outDir);
    List<String> dimensionNames = schema.getDimensionNames();
    List<String> metricNames = schema.getMetricNames();

    dimensionSizeBytes = dimensionNames.size() * Integer.SIZE / 8;
    dimensionTypes = new ArrayList<>();
    this.numDimensions = dimensionNames.size();
    dimensionNameToIndexMap = new HashMap<>();
    dictionaryMap = new HashMap<>();
    for (int i = 0; i < dimensionNames.size(); i++) {
      String dimensionName = dimensionNames.get(i);
      dimensionNameToIndexMap.put(dimensionName, i);
      dimensionTypes.add(schema.getFieldSpecFor(dimensionName).getDataType());
      HashBiMap<Object, Integer> dictionary = HashBiMap.create();
      dictionaryMap.put(schema.getDimensionNames().get(i), dictionary);
    }
    //treat time column as just another dimension, most likely we will never split on this dimension
    if (schema.getTimeColumnName() != null) {
      dimensionTypes.add(schema.getTimeFieldSpec().getDataType());
      dimensionNameToIndexMap.put(schema.getTimeColumnName(), numDimensions);
      numDimensions = numDimensions + 1;
      HashBiMap<Object, Integer> dictionary = HashBiMap.create();
      dictionaryMap.put(schema.getTimeColumnName(), dictionary);
      dimensionSizeBytes = dimensionSizeBytes + Integer.SIZE / 8;
    }
    metricTypes = new ArrayList<>();
    this.numMetrics = metricNames.size();
    metricNameToIndexMap = new HashMap<>();
    metricSizeBytes = 0;
    for (int i = 0; i < metricNames.size(); i++) {
      String metricName = metricNames.get(i);
      metricNameToIndexMap.put(metricName, i);
      DataType dataType = schema.getFieldSpecFor(metricName).getDataType();
      metricTypes.add(dataType);
      metricSizeBytes += dataType.size();
    }

    builderConfig.getOutDir().mkdirs();
    dataFile = new File(outDir, "star-tree.buf");
    dataBuffer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile)));
    this.starTree = new StarTreeIndexNode();
    this.starTree.setDimensionName(StarTreeIndexNode.all());
    this.starTree.setDimensionValue(StarTreeIndexNode.all());
    this.starTree.setLevel(0);
  }

  public GenericRow toGenericRow(DimensionBuffer dimensionKey, MetricBuffer metricsHolder) {
    GenericRow row = new GenericRow();
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < schema.getDimensionNames().size(); i++) {
      String dimName = schema.getDimensionNames().get(i);
      BiMap<Integer, Object> inverse = dictionaryMap.get(dimName).inverse();
      Object dimValue = inverse.get(dimensionKey.getDimension(i));
      if (dimValue == null) {
        //LOG.info("Found null entry for {}", dimName);
        dimValue = "ALL";
      }
      map.put(dimName, dimValue);
    }
    if (schema.getTimeColumnName() != null) {
      int index = numDimensions - 1;
      BiMap<Integer, Object> inverse = dictionaryMap.get(schema.getTimeColumnName()).inverse();
      map.put(schema.getTimeColumnName(), inverse.get(dimensionKey.getDimension(index)));
    }
    for (int i = 0; i < numMetrics; i++) {
      String metName = schema.getMetricNames().get(i);
      map.put(metName, metricsHolder.get(i));
    }
    row.init(map);
    return row;
  }

  public void append(GenericRow row) throws Exception {
    DimensionBuffer dimension = new DimensionBuffer(numDimensions);
    for (int i = 0; i < schema.getDimensionFieldSpecs().size(); i++) {
      DimensionFieldSpec dimensionFieldSpec = schema.getDimensionFieldSpecs().get(i);
      String dimName = schema.getDimensionNames().get(i);
      Map<Object, Integer> dictionary = dictionaryMap.get(dimName);
      Object dimValue = row.getValue(dimName);
      if (dimValue == null) {
        //TODO: Have another default value to represent STAR. Using default value to represent STAR as of now. 
        //It does not matter during query execution, since we know that values is STAR from the star tree
        dimValue = dimensionFieldSpec.getDefaultNullValue();
      }
      if (!dictionary.containsKey(dimValue)) {
        dictionary.put(dimValue, dictionary.size());
      }
      dimension.setDimension(i, dictionary.get(dimValue));
    }
    if (schema.getTimeColumnName() != null) {
      Map<Object, Integer> dictionary = dictionaryMap.get(schema.getTimeColumnName());
      Object timeColValue = row.getValue(schema.getTimeColumnName());
      if (!dictionary.containsKey(timeColValue)) {
        dictionary.put(timeColValue, dictionary.size());
      }
      dimension.setDimension(schema.getDimensionFieldSpecs().size(), dictionary.get(timeColValue));
    }
    Number[] numbers = new Number[numMetrics];
    for (int i = 0; i < numMetrics; i++) {
      String metName = schema.getMetricNames().get(i);
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
    if (splitOrder == null || splitOrder.isEmpty()) {
      splitOrder = computeDefaultSplitOrder();
    }
    LOG.info("Split order:{}", splitOrder);
    long start = System.currentTimeMillis();
    dataBuffer.flush();
    sort(dataFile, 0, rawRecordCount);
    constructStarTree(starTree, 0, rawRecordCount, 0, dataFile);
    long end = System.currentTimeMillis();
    LOG.info("Took {} ms to build star tree index. Original records:{} Materialized record:{}", (end - start),
        rawRecordCount, aggRecordCount);
    File treeBinary = new File(outDir, "star-tree.bin");
    LOG.info("Saving tree binary at: {} ", treeBinary);
    starTree.writeTree(new BufferedOutputStream(new FileOutputStream(treeBinary)));
    StarTreeIndexNode.printTree(starTree, 0);
    LOG.info("Finished build tree. out dir: {} ", outDir);
    dataBuffer.close();
  }

  private List<String> computeDefaultSplitOrder() {
    splitOrder = new ArrayList<>();
    //include only the dimensions not time column
    for (DimensionFieldSpec spec : schema.getDimensionFieldSpecs()) {
      splitOrder.add(spec.getName());
    }
    Collections.sort(splitOrder, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return dictionaryMap.get(o2).size() - dictionaryMap.get(o1).size(); //descending
      }
    });
    return splitOrder;
  }

  /*
   * Sorts the file on all dimensions
   */
  private void sort(File file, int startDocId, int endDocId) throws IOException {
    if (debugMode) {
      printFile(file, startDocId, endDocId);
    }
    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes);
    dataSorter.sort(startDocId, endDocId, 0, dimensionSizeBytes);
    if (debugMode) {
      printFile(file, startDocId, endDocId);
    }
  }

  private void printFile(File file, int startDocId, int endDocId) throws IOException {
    LOG.info("Contents of file:{} from:{} to:{}", file.getName(), startDocId, endDocId);
    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes);
    Iterator<Pair<byte[], byte[]>> iterator = dataSorter.iterator(startDocId, endDocId);
    while (iterator.hasNext()) {
      Pair<byte[], byte[]> next = iterator.next();
      LOG.info("{}, {}", DimensionBuffer.fromBytes(next.getLeft()),
          MetricBuffer.fromBytes(next.getRight(), metricTypes));
    }
  }

  private int constructStarTree(StarTreeIndexNode node, int startDocId, int endDocId, int level, File file)
      throws Exception {
    node.setStartDocumentId(startDocId);
    int docsAdded = 0;
    if (level == splitOrder.size() - 1) {
      return 0;
    }
    String splitDimensionName = splitOrder.get(level);
    Integer splitDimensionId = dimensionNameToIndexMap.get(splitDimensionName);
    LOG.info("Building tree at level:{} using file:{} from startDoc:{} endDocId:{} splitting on dimension:{}", level,
        file.getName(), startDocId, endDocId, splitDimensionName);
    Map<Integer, IntPair> sortGroupBy = groupBy(startDocId, endDocId, splitDimensionId, file);
    LOG.info("Group stats:{}", sortGroupBy);
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

      IntPair range = sortGroupBy.get(childDimensionValue);
      if (range.getRight() - range.getLeft() > maxLeafRecords) {
        docsAdded += constructStarTree(child, range.getLeft(), range.getRight(), level + 1, file);
      } else {
        child.setStartDocumentId(range.getLeft());
        child.setEndDocumentId(range.getRight());
      }
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
    LOG.info("Added {} additional records at level {}", rowsAdded, level);
    //flush
    dataBuffer.flush();
    if (rowsAdded >= maxLeafRecords) {
      sort(dataFile, startOffset, startOffset + rowsAdded);
      docsAdded += constructStarTree(starChild, startOffset, startOffset + rowsAdded, level + 1, dataFile);
    } else {
      starChild.setStartDocumentId(startOffset);
      starChild.setEndDocumentId(startOffset + rowsAdded);
    }
    node.setEndDocumentId(endDocId + docsAdded);
    if (node.getEndDocumentId() < node.getStartDocumentId()) {
      System.out.println("ERROR");
    }
    return docsAdded;
  }

  /**
   * Assumes the file is already sorted, returns the unique combinations after removing a specified dimension. 
   * Aggregates the metrics for each unique combination, currently only sum is supported by default
   * @param startDocId
   * @param endDocId
   * @param hasStarParent
   * @return
   */
  private Iterator<Pair<DimensionBuffer, MetricBuffer>> uniqueCombinations(int startDocId, int endDocId, File file,
      int splitDimensionId) throws Exception {
    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes);
    Iterator<Pair<byte[], byte[]>> iterator1 = dataSorter.iterator(startDocId, endDocId);
    File tempFile = new File(outDir, file.getName() + "_" + startDocId + "_" + endDocId + ".unique.tmp");
    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)));
    while (iterator1.hasNext()) {
      Pair<byte[], byte[]> next = iterator1.next();
      byte[] dimensionBuffer = next.getLeft();
      byte[] metricBuffer = next.getRight();
      DimensionBuffer dimensions = DimensionBuffer.fromBytes(dimensionBuffer);
      for (int i = 0; i < numDimensions; i++) {
        if (i == splitDimensionId) {
          dos.writeInt(StarTreeIndexNode.all());
        } else {
          dos.writeInt(dimensions.getDimension(i));
        }
      }
      dos.write(metricBuffer);
    }
    dos.close();
    dataSorter = new StarTreeDataTable(tempFile, dimensionSizeBytes, metricSizeBytes);
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
   * @param dimensionToSplitOn
   */
  private Map<Integer, IntPair> groupBy(int startDocId, int endDocId, Integer dimension, File file) {
    StarTreeDataTable dataSorter = new StarTreeDataTable(file, dimensionSizeBytes, metricSizeBytes);
    return dataSorter.groupByIntColumnCount(startDocId, endDocId, dimension);
  }

  /**
   * Iterator to iterate over the records from startDocId to endDocId
   */
  @Override
  public Iterator<GenericRow> iterator(final int startDocId, final int endDocId) throws Exception {
    StarTreeDataTable dataSorter = new StarTreeDataTable(dataFile, dimensionSizeBytes, metricSizeBytes);
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
    toJson(json, starTree, schema, dictionaryMap);
    return json;
  }

  private void toJson(JSONObject json, StarTreeIndexNode node, Schema schema,
      Map<String, HashBiMap<Object, Integer>> dictionaryMap) throws Exception {
    String dimName = "ALL";
    Object dimValue = "ALL";
    if (node.getDimensionName() != StarTreeIndexNode.all()) {
      dimName = schema.getDimensionFieldSpecs().get(node.getDimensionName()).getName();
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
        toJson(childJson, childNode, schema, dictionaryMap);
        childJsons[index++] = childJson;
      }
      json.put("nodes", childJsons);
    }
  }

  public StarTreeIndexNode getStarTree() {
    return starTree;
  }

  @Override
  public StarTreeIndexNode getTree() {
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
  public List<String> getSplitOrder() {
    return splitOrder;
  }

}
