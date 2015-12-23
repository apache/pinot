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
package com.linkedin.pinot.core.segment.creator.impl;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.CARDINALITY;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.COLUMN_TYPE;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.DATA_TYPE;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.HAS_NULL_VALUE;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.IS_SORTED;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.MAX_MULTI_VALUE_ELEMTS;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.TOTAL_DOCS;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.TOTAL_NUMBER_OF_ENTRIES;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.DIMENSIONS;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.METRICS;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.TABLE_NAME;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.SEGMENT_NAME;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.TIME_INTERVAL;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.TIME_UNIT;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.MultiValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.SegmentCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;


/**
 * Segment creator which writes data in a columnar form.
 *
 * Nov 9, 2014
 */

public class SegmentColumnarIndexCreator implements SegmentCreator {
  // TODO Refactor class name to match interface name
  private SegmentGeneratorConfig config;
  private Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  private Map<String, SegmentDictionaryCreator> dictionaryCreatorMap;
  private Map<String, ForwardIndexCreator> forwardIndexCreatorMap;
  private Map<String, InvertedIndexCreator> invertedIndexCreatorMap;
  private String segmentName;

  private Schema schema;
  private File file;
  private int totalDocs;
  private int docIdCounter;
  private Map<String, Map<Object, Object>> dictionaryCache = new HashMap<String, Map<Object, Object>>();

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, int totalDocs, File outDir)
      throws Exception {
    docIdCounter = 0;
    config = segmentCreationSpec;
    this.indexCreationInfoMap = indexCreationInfoMap;
    dictionaryCreatorMap = new HashMap<String, SegmentDictionaryCreator>();
    forwardIndexCreatorMap = new HashMap<String, ForwardIndexCreator>();
    this.indexCreationInfoMap = indexCreationInfoMap;
    invertedIndexCreatorMap = new HashMap<String, InvertedIndexCreator>();
    file = outDir;

    // Check that the output directory does not exist
    if (file.exists()) {
      throw new RuntimeException("Segment output directory " + file.getAbsolutePath() + " already exists.");
    }

    file.mkdir();

    this.schema = schema;

    this.totalDocs = totalDocs;

    // Initialize and build dictionaries
    for (final FieldSpec spec : schema.getAllFieldSpecs()) {
      final ColumnIndexCreationInfo info = indexCreationInfoMap.get(spec.getName());
      if (info.isCreateDictionary()) {
        dictionaryCreatorMap.put(spec.getName(),
            new SegmentDictionaryCreator(info.hasNulls(), info.getSortedUniqueElementsArray(), spec, file));
      } else {
        throw new RuntimeException("Creation of indices without dictionaries is not implemented!");
      }
    }

    // For each column, build its dictionary and initialize a forwards and an inverted index
    for (final String column : dictionaryCreatorMap.keySet()) {
      dictionaryCreatorMap.get(column).build();
      dictionaryCache.put(column, new HashMap<Object, Object>());
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(column);
      int uniqueValueCount = indexCreationInfo.getDistinctValueCount();
      if (schema.getFieldSpecFor(column).isSingleValueField()) {
        if (indexCreationInfo.isSorted()) {
          forwardIndexCreatorMap.put(column,
              new SingleValueSortedForwardIndexCreator(file, uniqueValueCount,
                  schema.getFieldSpecFor(column)));
        } else {
          forwardIndexCreatorMap.put(
              column,
              new SingleValueUnsortedForwardIndexCreator(schema.getFieldSpecFor(column), file,
                  uniqueValueCount, totalDocs, indexCreationInfo.getTotalNumberOfEntries(),
                  indexCreationInfo.hasNulls()));
        }
      } else {
        forwardIndexCreatorMap.put(
            column,
            new MultiValueUnsortedForwardIndexCreator(schema.getFieldSpecFor(column), file,
                uniqueValueCount, totalDocs, indexCreationInfo.getTotalNumberOfEntries(),
                indexCreationInfo.hasNulls()));
      }
    }

    for (String column : config.getInvertedIndexCreationColumns()) {
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(column);
      int uniqueValueCount = indexCreationInfo.getDistinctValueCount();
      OffHeapBitmapInvertedIndexCreator invertedIndexCreator =
          new OffHeapBitmapInvertedIndexCreator(file, uniqueValueCount, totalDocs,
              indexCreationInfo.getTotalNumberOfEntries(), schema.getFieldSpecFor(column));
      invertedIndexCreatorMap.put(column, invertedIndexCreator);
    }
  }

  @Override
  public void indexRow(GenericRow row) {
    for (final String column : dictionaryCreatorMap.keySet()) {

      Object columnValueToIndex = row.getValue(column);
      if (schema.getFieldSpecFor(column).isSingleValueField()) {
        int dictionaryIndex = dictionaryCreatorMap.get(column).indexOfSV(columnValueToIndex);
        ((SingleValueForwardIndexCreator)forwardIndexCreatorMap.get(column)).index(docIdCounter, dictionaryIndex);

        // TODO : {refactor inverted index addition}
        if (invertedIndexCreatorMap.containsKey(column)) {
          invertedIndexCreatorMap.get(column).add(docIdCounter, dictionaryIndex);
        }
      } else {
        int[] dictionaryIndex = dictionaryCreatorMap.get(column).indexOfMV(columnValueToIndex);
        ((MultiValueForwardIndexCreator)forwardIndexCreatorMap.get(column)).index(docIdCounter, dictionaryIndex);

        // TODO : {refactor inverted index addition}
        if (invertedIndexCreatorMap.containsKey(column)) {
          invertedIndexCreatorMap.get(column).add(docIdCounter, dictionaryIndex);
        }
      }
    }
    docIdCounter++;
  }

  @Override
  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public void seal() throws ConfigurationException, IOException {
    for (final String column : forwardIndexCreatorMap.keySet()) {
      forwardIndexCreatorMap.get(column).close();
      dictionaryCreatorMap.get(column).close();
    }

    // The map is only initialized for columns that have inverted index creation enabled.
    for (final String invertedColumn : invertedIndexCreatorMap.keySet()) {
      invertedIndexCreatorMap.get(invertedColumn).seal();
    }
    writeMetadata();
  }

  void writeMetadata() throws ConfigurationException {
    final PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(file, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_NAME, segmentName);
    properties.setProperty(TABLE_NAME, config.getTableName());
    properties.setProperty(DIMENSIONS, config.getDimensions());
    properties.setProperty(METRICS, config.getMetrics());
    properties.setProperty(TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.setProperty(TIME_INTERVAL, "not_there");
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(totalDocs));

    String timeColumn = config.getTimeColumnName();
    if (indexCreationInfoMap.get(timeColumn) != null) {
      properties.setProperty(SEGMENT_START_TIME, indexCreationInfoMap.get(timeColumn).getMin());
      properties.setProperty(SEGMENT_END_TIME, indexCreationInfoMap.get(timeColumn).getMax());
      properties.setProperty(TIME_UNIT, config.getTimeUnitForSegment());
    }

    if (config.containsCustomPropertyWithKey(SEGMENT_START_TIME)) {
      properties.setProperty(SEGMENT_START_TIME, config.getStartTime());
    }
    if (config.containsCustomPropertyWithKey(SEGMENT_END_TIME)) {
      properties.setProperty(SEGMENT_END_TIME, config.getEndTime());
    }
    if (config.containsCustomPropertyWithKey(TIME_UNIT)) {
      properties.setProperty(TIME_UNIT, config.getTimeUnitForSegment());
    }

    for (final String key : config.getAllCustomKeyValuePair().keySet()) {
      properties.setProperty(key, config.getAllCustomKeyValuePair().get(key));
    }

    for (final String column : indexCreationInfoMap.keySet()) {
      final ColumnIndexCreationInfo columnIndexCreationInfo = indexCreationInfoMap.get(column);
      final int uniqueValueCount = columnIndexCreationInfo.getDistinctValueCount();
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, CARDINALITY),
          String.valueOf(uniqueValueCount));
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DATA_TYPE),
          schema.getFieldSpecFor(column).getDataType().toString());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, BITS_PER_ELEMENT),
          String.valueOf(SingleValueUnsortedForwardIndexCreator.getNumOfBits(uniqueValueCount)));

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
