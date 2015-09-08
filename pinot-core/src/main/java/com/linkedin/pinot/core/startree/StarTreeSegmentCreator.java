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

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.StarTree.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;

import com.google.common.base.Joiner;
import com.linkedin.pinot.common.data.*;
import com.linkedin.pinot.common.utils.PrimitiveArrayUtils;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.*;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.inv.BitmapInvertedIndexCreator;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class StarTreeSegmentCreator implements SegmentCreator {
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeSegmentCreator.class);

  private StarTreeIndexSpec starTreeIndexSpec; // TODO: Support multiple trees
  private RecordReader recordReader;
  private StarTreeBuilder starTreeBuilder;

  // From #init, #setSegmentName
  private SegmentGeneratorConfig config;
  private Map<String, ColumnIndexCreationInfo> columnInfo;
  private Schema schema;
  private File outDir;
  private File starTreeDir;
  private String segmentName;

  // Used for indexRow
  private Set<StarTreeIndexNode> currentMatchingNodes;

  // Computed
  private List<String> splitOrder;
  private Map<String, Integer> starTreeDimensionDictionary;
  private Map<String, Integer> starTreeMetricDictionary;

  // Dictionary / Index creators
  private Map<String, SegmentDictionaryCreator> dictionaryCreatorMap;
  private Map<String, ForwardIndexCreator> forwardIndexCreatorMap;
  private Map<String, InvertedIndexCreator> invertedIndexCreatorMap;
  private Map<String, ForwardIndexCreator> aggregateForwardIndexCreatorMap;
  private Map<String, InvertedIndexCreator> aggregateInvertedIndexCreatorMap;

  public StarTreeSegmentCreator(StarTreeIndexSpec starTreeIndexSpec, RecordReader recordReader) {
    this.starTreeIndexSpec = starTreeIndexSpec;
    this.recordReader = recordReader;
    this.dictionaryCreatorMap = new HashMap<String, SegmentDictionaryCreator>();
    this.starTreeBuilder = new DefaultStarTreeBuilder();
    this.currentMatchingNodes = new HashSet<StarTreeIndexNode>();
    this.forwardIndexCreatorMap = new HashMap<String, ForwardIndexCreator>();
    this.invertedIndexCreatorMap = new HashMap<String, InvertedIndexCreator>();
    this.aggregateForwardIndexCreatorMap = new HashMap<String, ForwardIndexCreator>();
    this.aggregateInvertedIndexCreatorMap = new HashMap<String, InvertedIndexCreator>();
  }

  @Override
  public void init(SegmentGeneratorConfig config,
                   Map<String, ColumnIndexCreationInfo> columnInfo,
                   Schema schema,
                   int totalDocs,
                   File outDir) throws Exception {
    // Member variables
    this.config = config;
    this.columnInfo = columnInfo;
    this.schema = schema;
    this.outDir = outDir;
    this.starTreeDimensionDictionary = new HashMap<String, Integer>();
    this.starTreeMetricDictionary = new HashMap<String, Integer>();

    // Dictionaries (will go in root segment)
    initializeAndBuildDictionaries(schema, columnInfo, outDir);

    // Compute dimension dictionary
    for (int i = 0; i < schema.getDimensionNames().size(); i++) {
      starTreeDimensionDictionary.put(schema.getDimensionNames().get(i), i);
    }
    LOG.info("StarTree dimension dictionary: {}", starTreeDimensionDictionary);

    // Compute the metric dictionary
    for (int i = 0; i < schema.getMetricNames().size(); i++) {
      starTreeMetricDictionary.put(schema.getMetricNames().get(i), i);
    }
    LOG.info("StarTree metric dictionary: {}", starTreeDimensionDictionary);

    // Compute StarTree split order
    splitOrder = computeSplitOrder(columnInfo);
    LOG.info("Computed split order {}", splitOrder);
    List<Integer> splitOrderIndexes = new ArrayList<Integer>();
    for (String dimensionName : splitOrder) {
      Integer dimensionId = starTreeDimensionDictionary.get(dimensionName);
      splitOrderIndexes.add(dimensionId);
    }
    Collections.reverse(splitOrderIndexes);

    // StarTree builder / table
    StarTreeTable table = new LinkedListStarTreeTable(); // TODO: ByteBuffer-based
    StarTreeDocumentIdMap documentIdMap = new HashMapStarTreeDocumentIdMap(); // TODO: ByteBuffer-based
    starTreeBuilder.init(splitOrderIndexes, starTreeIndexSpec.getMaxLeafRecords(), table, documentIdMap);

    // Build the StarTree structure and table
    LOG.info("Building StarTree table...");
    int count = 0;
    long startMillis = System.currentTimeMillis();
    recordReader.rewind();
    while (recordReader.hasNext()) {
      GenericRow row = recordReader.next();
      StarTreeTableRow starTreeTableRow = extractValues(row);
      starTreeBuilder.append(starTreeTableRow);
      count++;
    }
    long endMillis = System.currentTimeMillis();
    LOG.info("Finished building StarTree table ({} documents, took {} ms)", count, endMillis - startMillis);

    LOG.info("Building StarTree (computing aggregates)...");
    startMillis = System.currentTimeMillis();
    starTreeBuilder.build();
    endMillis = System.currentTimeMillis();
    LOG.info("Finished building StarTree, took {} ms", endMillis - startMillis);

    // Re-compute the unique values for metrics including aggregates to allow for dictionary encoding
    LOG.info("Re-computing unique metric values for dictionary encoding...");
    startMillis = System.currentTimeMillis();
    Map<String, Set<Object>> uniqueMetricValues = computeUniqueMetricValues();
    resetMetricDictionaries(uniqueMetricValues);
    endMillis = System.currentTimeMillis();
    LOG.info("Finished re-computing unique metric values (took {} ms)", endMillis - startMillis);

    // StarTree directory
    starTreeDir = new File(outDir, V1Constants.STARTREE_DIR);
    if (!starTreeDir.mkdir()) {
      throw new RuntimeException("Could not create star tree directory " + starTreeDir.getAbsolutePath());
    }

    // For each column, build its dictionary and initialize a forwards and an inverted index for raw / agg segment
    int totalAggDocs = starTreeBuilder.getTotalAggregateDocumentCount();
    int totalRawDocs = starTreeBuilder.getTotalRawDocumentCount();
    for (final String column : dictionaryCreatorMap.keySet()) {
      ColumnIndexCreationInfo indexCreationInfo = columnInfo.get(column);

      int uniqueValueCount = indexCreationInfo.getDistinctValueCount();
      if (schema.getMetricNames().contains(column)) {
        // Use the unique values including the new aggregate values
        uniqueValueCount = uniqueMetricValues.get(column).toArray().length;
      }

      if (schema.getFieldSpecFor(column).isSingleValueField()) {
        if (indexCreationInfo.isSorted()) {
          forwardIndexCreatorMap.put(column,
              new SingleValueSortedForwardIndexCreator(outDir, uniqueValueCount, schema.getFieldSpecFor(column)));
          aggregateForwardIndexCreatorMap.put(column,
              new SingleValueSortedForwardIndexCreator(starTreeDir, uniqueValueCount, schema.getFieldSpecFor(column)));
        } else {
          forwardIndexCreatorMap.put(
              column,
              new SingleValueUnsortedForwardIndexCreator(schema.getFieldSpecFor(column), outDir,
                  uniqueValueCount, totalRawDocs, indexCreationInfo.getTotalNumberOfEntries(), indexCreationInfo.hasNulls()));
          aggregateForwardIndexCreatorMap.put(
              column,
              new SingleValueUnsortedForwardIndexCreator(schema.getFieldSpecFor(column), starTreeDir, uniqueValueCount,
                  totalAggDocs, indexCreationInfo.getTotalNumberOfEntries(), indexCreationInfo.hasNulls()));
        }
      } else {
        forwardIndexCreatorMap.put(
            column,
            new MultiValueUnsortedForwardIndexCreator(schema.getFieldSpecFor(column), outDir,
                uniqueValueCount, totalRawDocs, indexCreationInfo.getTotalNumberOfEntries(),
                indexCreationInfo.hasNulls()));
        aggregateForwardIndexCreatorMap.put(
            column,
            new MultiValueUnsortedForwardIndexCreator(schema.getFieldSpecFor(column), starTreeDir,
                uniqueValueCount, totalAggDocs, indexCreationInfo.getTotalNumberOfEntries(),
                indexCreationInfo.hasNulls()));
      }

      if (config.createInvertedIndexEnabled()) {
        invertedIndexCreatorMap.put(
            column,
            new BitmapInvertedIndexCreator(outDir, uniqueValueCount, schema.getFieldSpecFor(column)));
        aggregateInvertedIndexCreatorMap.put(
            column,
            new BitmapInvertedIndexCreator(starTreeDir, uniqueValueCount, schema.getFieldSpecFor(column)));
      }
    }
  }

  @Override
  public void indexRow(GenericRow row) {
    // Find matching leaves in StarTree for row
    currentMatchingNodes.clear();
    StarTreeTableRow tableRow = extractValues(row);
    findMatchingLeaves(starTreeBuilder.getTree(), tableRow.getDimensions(), currentMatchingNodes);

    // Only write the raw value, maintaining sort order (we will write aggregates when sealing)
    for (StarTreeIndexNode node : currentMatchingNodes) {
      Map<Integer, Integer> pathValues = node.getPathValues();
      if (!pathValues.containsValue(StarTreeIndexNode.all())) {
        StarTreeTableRange range = starTreeBuilder.getDocumentIdRange(node.getNodeId());
        StarTreeTable subTable = starTreeBuilder.getTable().view(range.getStartDocumentId(), range.getDocumentCount());

        Integer nextMatchingDocumentId = starTreeBuilder.getNextDocumentId(tableRow.getDimensions());
        if (nextMatchingDocumentId == null) {
          throw new IllegalStateException("Could not assign document ID for row " + tableRow);
        }

        // Write using that document ID to all columns
        for (final String column : dictionaryCreatorMap.keySet()) {
          Object columnValueToIndex = row.getValue(column);
          if (schema.getFieldSpecFor(column).isSingleValueField()) {
            System.out.println(column + ": " + columnValueToIndex);
            int dictionaryIndex = dictionaryCreatorMap.get(column).indexOfSV(columnValueToIndex);
            ((SingleValueForwardIndexCreator)forwardIndexCreatorMap.get(column)).index(nextMatchingDocumentId, dictionaryIndex);
            if (config.createInvertedIndexEnabled()) {
              invertedIndexCreatorMap.get(column).add(nextMatchingDocumentId, (Object) dictionaryIndex);
            }
          } else {
            int[] dictionaryIndex = dictionaryCreatorMap.get(column).indexOfMV(columnValueToIndex);
            ((MultiValueForwardIndexCreator)forwardIndexCreatorMap.get(column)).index(nextMatchingDocumentId, dictionaryIndex);
            if (config.createInvertedIndexEnabled()) {
              invertedIndexCreatorMap.get(column).add(nextMatchingDocumentId, dictionaryIndex);
            }
          }
        }
      }
    }
  }

  @Override
  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public void seal() throws ConfigurationException, IOException {
    // Write all the aggregate rows to the aggregate segment
    LOG.info("Writing aggregate segment...");
    long startMillis = System.currentTimeMillis();
    int currentAggregateDocumentId = 0;
    Iterator<StarTreeTableRow> itr = starTreeBuilder.getTable().getAllCombinations();
    while (itr.hasNext()) {
      StarTreeTableRow next = itr.next();
      if (next.getDimensions().contains(StarTreeIndexNode.all())) {
        // Write using that document ID to all columns
        for (final String column : dictionaryCreatorMap.keySet()) {
          Object dictionaryIndex = null; // TODO: Is this okay?

          if (starTreeDimensionDictionary.containsKey(column)) {
            // Index the dimension value
            Integer dimensionId = starTreeDimensionDictionary.get(column);
            Integer dimensionValue = next.getDimensions().get(dimensionId);
            if (dimensionValue == StarTreeIndexNode.all()) {
              // Use all value
              Object allValue = StarTreeIndexNode.getAllValue(schema.getFieldSpecFor(column));
              if (schema.getFieldSpecFor(column).isSingleValueField()) {
                dictionaryIndex = dictionaryCreatorMap.get(column).indexOfSV(allValue);
              } else {
                dictionaryIndex = dictionaryCreatorMap.get(column).indexOfMV(allValue);
              }
            } else {
              dictionaryIndex = dimensionValue;
            }
          } else if (starTreeMetricDictionary.containsKey(column)) {
            // Index the aggregate metric
            Integer metricId = starTreeMetricDictionary.get(column);
            Object columnValueToIndex = next.getMetrics().get(metricId);
            if (schema.getFieldSpecFor(column).isSingleValueField()) {
              dictionaryIndex = dictionaryCreatorMap.get(column).indexOfSV(columnValueToIndex);
            } else {
              dictionaryIndex = dictionaryCreatorMap.get(column).indexOfMV(columnValueToIndex);
            }
          } else {
            // Just index the raw value
            Object columnValueToIndex = StarTreeIndexNode.getAllValue(schema.getFieldSpecFor(column));
            if (schema.getFieldSpecFor(column).isSingleValueField()) {
              dictionaryIndex = dictionaryCreatorMap.get(column).indexOfSV(columnValueToIndex);
            } else {
              dictionaryIndex = dictionaryCreatorMap.get(column).indexOfMV(columnValueToIndex);
            }
          }

          if (schema.getFieldSpecFor(column).isSingleValueField()) {
            ((SingleValueForwardIndexCreator)aggregateForwardIndexCreatorMap.get(column))
                .index(currentAggregateDocumentId, (Integer) dictionaryIndex);
          } else {
            ((MultiValueForwardIndexCreator)aggregateForwardIndexCreatorMap.get(column))
                .index(currentAggregateDocumentId, (int[]) dictionaryIndex);
          }

          if (config.createInvertedIndexEnabled()) {
            aggregateInvertedIndexCreatorMap.get(column).add(currentAggregateDocumentId, dictionaryIndex);
          }
        }
        currentAggregateDocumentId++;
      }
    }
    long endMillis = System.currentTimeMillis();
    LOG.info("Done writing aggregate segment (took {} ms)", endMillis - startMillis);

    for (final String column : forwardIndexCreatorMap.keySet()) {
      forwardIndexCreatorMap.get(column).close();
      if (config.createInvertedIndexEnabled()) {
        invertedIndexCreatorMap.get(column).seal();
      }
      dictionaryCreatorMap.get(column).close();
    }

    for (final String column : aggregateForwardIndexCreatorMap.keySet()) {
      aggregateForwardIndexCreatorMap.get(column).close();
      if (config.createInvertedIndexEnabled()) {
        aggregateInvertedIndexCreatorMap.get(column).seal();
      }
      // n.b. The dictionary from raw data is used
    }

    writeMetadata(outDir, starTreeBuilder.getTotalRawDocumentCount());

    // Write star tree
    LOG.info("Writing " + V1Constants.STARTREE_FILE);
    startMillis = System.currentTimeMillis();
    File starTreeFile = new File(starTreeDir, V1Constants.STARTREE_FILE);
    OutputStream starTreeOutputStream = new FileOutputStream(starTreeFile);
    starTreeBuilder.getTree().writeTree(starTreeOutputStream);
    starTreeOutputStream.close();
    endMillis = System.currentTimeMillis();
    LOG.info("Wrote StarTree file (took {} ms)", endMillis - startMillis);

    // Copy the dictionary files into startree directory
    // n.b. this is done so the segment is as stand-alone as possible, though could be removed as an optimization
    File[] dictionaryFiles = outDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(V1Constants.Dict.FILE_EXTENTION);
      }
    });
    for (File dictionaryFile : dictionaryFiles) {
      FileUtils.copyFile(dictionaryFile, new File(starTreeDir, dictionaryFile.getName()));
    }

    // Write star tree metadata
    writeMetadata(starTreeDir, starTreeBuilder.getTotalAggregateDocumentCount());
  }

  /** Returns the user-defined split order, or dimensions in order of descending cardinality (removes excludes too) */
  private List<String> computeSplitOrder(final Map<String, ColumnIndexCreationInfo> columnInfo) {
    List<String> splitOrder;
    if (starTreeIndexSpec.getSplitOrder() == null) {
      splitOrder = new ArrayList<String>(schema.getDimensionNames());
      Collections.sort(splitOrder, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          int o1DistinctValueCount = columnInfo.get(o1).getDistinctValueCount();
          int o2DistinctValueCount = columnInfo.get(o2).getDistinctValueCount();
          return o2DistinctValueCount - o1DistinctValueCount; // descending
        }
      });
    } else {
      splitOrder = new ArrayList<String>(starTreeIndexSpec.getSplitOrder());
    }

    if (starTreeIndexSpec.getSplitExcludes() != null) {
      splitOrder.removeAll(starTreeIndexSpec.getSplitExcludes());
    }

    return splitOrder;
  }

  /** Converts a raw row into its (possibly partial) dimension and complete metric values */
  private StarTreeTableRow extractValues(GenericRow row) {
    List<Integer> dimensions = new ArrayList<Integer>();
    for (String dimensionName : schema.getDimensionNames()) {
      Integer valueId;
      if (schema.getFieldSpecFor(dimensionName).isSingleValueField()
          && !starTreeIndexSpec.getExcludedDimensions().contains(dimensionName)) {
        Object value = row.getValue(dimensionName);
        valueId = dictionaryCreatorMap.get(dimensionName).indexOfSV(value);
      } else {
        // Multi-value fields are not supported - always ALL
        valueId = V1Constants.STARTREE_ALL_NUMBER.intValue();
      }

      dimensions.add(valueId);
    }

    List<Number> metrics = new ArrayList<Number>(schema.getMetricNames().size());
    for (MetricFieldSpec metricFieldSpec : schema.getMetricFieldSpecs()) {
      Object value = row.getValue(metricFieldSpec.getName());
      switch (metricFieldSpec.getDataType()) {
        case INT:
          metrics.add((Integer) value);
          break;
        case LONG:
          metrics.add((Long) value);
          break;
        case DOUBLE:
          metrics.add((Double) value);
          break;
        case FLOAT:
          metrics.add((Float) value);
          break;
        default:
          throw new IllegalStateException("Unsupported data type " + metricFieldSpec.getDataType());
      }
    }

    return new StarTreeTableRow(dimensions, metrics);
  }

  /** Initialize dictionaries for dimension values */
  private void initializeAndBuildDictionaries(Schema schema, Map<String, ColumnIndexCreationInfo> columnInfo, File file)
      throws Exception {
    for (final FieldSpec spec : schema.getAllFieldSpecs()) {
      final ColumnIndexCreationInfo info = columnInfo.get(spec.getName());
      if (info.isCreateDictionary()) {
        dictionaryCreatorMap.put(spec.getName(),
            new SegmentDictionaryCreator(info.hasNulls(), info.getSortedUniqueElementsArray(), spec, file));
      } else {
        throw new RuntimeException("Creation of indices without dictionaries is not implemented!");
      }
      dictionaryCreatorMap.get(spec.getName()).build();
    }

    // Add __ALL__ to dimension dictionaries
    for (DimensionFieldSpec spec : schema.getDimensionFieldSpecs()) {
      Object allValue = StarTreeIndexNode.getAllValue(spec);
      if (schema.getFieldSpecFor(spec.getName()).isSingleValueField()) {
        Object allIndex = dictionaryCreatorMap.get(spec.getName()).indexOfSV(allValue);
      } else {
        Object allIndex = dictionaryCreatorMap.get(spec.getName()).indexOfMV(allValue);
      }
    }
  }

  /** Collects all the StarTree leaves that match the provided dimension values */
  private void findMatchingLeaves(StarTreeIndexNode node, List<Integer> values, Set<StarTreeIndexNode> leaves) {
    if (node.isLeaf()) {
      leaves.add(node);
    } else {
      Integer value = values.get(node.getChildDimensionName());
      findMatchingLeaves(node.getChildren().get(value), values, leaves);
      findMatchingLeaves(node.getChildren().get(StarTreeIndexNode.all()), values, leaves);
    }
  }

  /**
   * Returns the unique metric values for each column.
   *
   * <p>
   *   The original unique values cannot be used because after aggregation, we almost certainly
   *   have new values to encode that were not present in the original data set.
   * </p>
   */
  private Map<String, Set<Object>> computeUniqueMetricValues() {
    Map<String, Set<Object>> uniqueMetricValues = new HashMap<String, Set<Object>>();

    Iterator<StarTreeTableRow> tableIterator = starTreeBuilder.getTable().getAllCombinations();
    while (tableIterator.hasNext()) {
      StarTreeTableRow row = tableIterator.next();

      for (int i = 0; i < schema.getMetricNames().size(); i++) {
        String metricName = schema.getMetricNames().get(i);
        Object metricValue = row.getMetrics().get(i);
        Set<Object> uniqueValues = uniqueMetricValues.get(metricName);
        if (uniqueValues == null) {
          uniqueValues = new HashSet<Object>();
          uniqueMetricValues.put(metricName, uniqueValues);
        }
        uniqueValues.add(metricValue);
      }
    }

    return uniqueMetricValues;
  }

  /**
   * Re-initializes only the metric dictionaries using the unique metric values (computed after aggregation).
   */
  private void resetMetricDictionaries(Map<String, Set<Object>> uniqueMetricValues) throws Exception {
    for (MetricFieldSpec spec : schema.getMetricFieldSpecs()) {
      String column = spec.getName();
      ColumnIndexCreationInfo info = columnInfo.get(column);

      // The new unique values
      Object[] valuesWithAggregates = uniqueMetricValues.get(column).toArray();
      Arrays.sort(valuesWithAggregates);

      // Reset dictionaries
      dictionaryCreatorMap.put(column, new SegmentDictionaryCreator(info.hasNulls(),
          PrimitiveArrayUtils.toPrimitive(valuesWithAggregates), spec, outDir));
      dictionaryCreatorMap.get(column).build();
    }
  }

  /** Constructs the segment metadata file, and writes in outputDir */
  private void writeMetadata(File outputDir, int totalDocs) throws ConfigurationException {
    final PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(outputDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_NAME, segmentName);
    properties.setProperty(TABLE_NAME, config.getTableName());
    properties.setProperty(DIMENSIONS, config.getDimensions());
    properties.setProperty(METRICS, config.getMetrics());
    properties.setProperty(TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.setProperty(TIME_INTERVAL, "not_there");
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(totalDocs));

    // StarTree
    Joiner csv = Joiner.on(",");
    properties.setProperty(SPLIT_ORDER, csv.join(splitOrder));
    properties.setProperty(SPLIT_EXCLUDES, csv.join(starTreeIndexSpec.getSplitExcludes()));
    properties.setProperty(MAX_LEAF_RECORDS, starTreeIndexSpec.getMaxLeafRecords());
    properties.setProperty(EXCLUDED_DIMENSIONS, csv.join(starTreeIndexSpec.getExcludedDimensions()));

    String timeColumn = config.getTimeColumnName();
    if (columnInfo.get(timeColumn) != null) {
      properties.setProperty(SEGMENT_START_TIME, columnInfo.get(timeColumn).getMin());
      properties.setProperty(SEGMENT_END_TIME, columnInfo.get(timeColumn).getMax());
      properties.setProperty(TIME_UNIT, config.getTimeUnitForSegment());
    }

    if (config.containsKey(SEGMENT_START_TIME)) {
      properties.setProperty(SEGMENT_START_TIME, config.getStartTime());
    }
    if (config.containsKey(SEGMENT_END_TIME)) {
      properties.setProperty(SEGMENT_END_TIME, config.getStartTime());
    }
    if (config.containsKey(TIME_UNIT)) {
      properties.setProperty(TIME_UNIT, config.getTimeUnitForSegment());
    }

    for (final String key : config.getAllCustomKeyValuePair().keySet()) {
      properties.setProperty(key, config.getAllCustomKeyValuePair().get(key));
    }

    for (final String column : columnInfo.keySet()) {
      final ColumnIndexCreationInfo columnIndexCreationInfo = columnInfo.get(column);
      final int distinctValueCount = columnIndexCreationInfo.getDistinctValueCount();
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, CARDINALITY),
          String.valueOf(distinctValueCount));
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DATA_TYPE),
          schema.getFieldSpecFor(column).getDataType().toString());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, BITS_PER_ELEMENT), String
          .valueOf(SingleValueUnsortedForwardIndexCreator.getNumOfBits(distinctValueCount)));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DICTIONARY_ELEMENT_SIZE),
          String.valueOf(dictionaryCreatorMap.get(column).getStringColumnMaxLength()));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, COLUMN_TYPE),
          String.valueOf(schema.getFieldSpecFor(column).getFieldType().toString()));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, IS_SORTED),
          String.valueOf(columnIndexCreationInfo.isSorted()));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, HAS_NULL_VALUE),
          String.valueOf(columnIndexCreationInfo.hasNulls()));
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_DICTIONARY),
          String.valueOf(columnIndexCreationInfo.isCreateDictionary()));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, HAS_INVERTED_INDEX),
          String.valueOf(true));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, IS_SINGLE_VALUED),
          String.valueOf(schema.getFieldSpecFor(column).isSingleValueField()));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, MAX_MULTI_VALUE_ELEMTS),
          String.valueOf(columnIndexCreationInfo.getMaxNumberOfMutiValueElements()));

      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES),
          String.valueOf(columnIndexCreationInfo.getTotalNumberOfEntries()));

    }

    properties.save();
  }
}
