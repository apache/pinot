package com.linkedin.pinot.core.segment.creator.impl;

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
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.SegmentCreator;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;


/**
 * Segment creator which writes data in a columnar form.
 *
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 9, 2014
 */

public class SegmentColumnarIndexCreator implements SegmentCreator {
  // TODO Refactor class name to match interface name
  private SegmentGeneratorConfig config;
  private Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  private Map<String, SegmentDictionaryCreator> dictionaryCreatorMap;
  private Map<String, SegmentForwardIndexCreatorImpl> forwardIndexCreatorMap;
  private Map<String, InvertedIndexCreator> invertedIndexCreatorMap;
  private String segmentName;

  private Schema schema;
  private File file;
  private int totalDocs;
  private int docIdCounter;

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap,
      Schema schema, int totalDocs, File outDir) throws Exception {
    docIdCounter = 0;
    config = segmentCreationSpec;
    this.indexCreationInfoMap = indexCreationInfoMap;
    dictionaryCreatorMap = new HashMap<String, SegmentDictionaryCreator>();
    forwardIndexCreatorMap = new HashMap<String, SegmentForwardIndexCreatorImpl>();
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
        dictionaryCreatorMap.put(
            spec.getName(),
            new SegmentDictionaryCreator(info.hasNulls(), info.getSortedUniqueElementsArray(), spec, file));
      } else {
        throw new RuntimeException("Creation of indices without dictionaries is not implemented!");
      }
    }

    // For each column, build its dictionary and initialize a forwards and an inverted index
    for (final String column : dictionaryCreatorMap.keySet()) {
      dictionaryCreatorMap.get(column).build();

      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(column);
      forwardIndexCreatorMap.put(column,
          new SegmentForwardIndexCreatorImpl(
              schema.getFieldSpecFor(column),
              file,
              indexCreationInfo.getSortedUniqueElementsArray().length,
              totalDocs,
              indexCreationInfo.getTotalNumberOfEntries(),
              indexCreationInfo.hasNulls()));

      invertedIndexCreatorMap.put(column,
          new SegmentInvertedIndexCreatorImpl(
              file,
              indexCreationInfo.getSortedUniqueElementsArray().length,
              schema.getFieldSpecFor(column)));
    }
  }

  @Override
  public void indexRow(GenericRow row) {
    for (final String column : dictionaryCreatorMap.keySet()) {
      Object dictionaryIndex = dictionaryCreatorMap.get(column).indexOf(row.getValue(column));
      forwardIndexCreatorMap.get(column).index(dictionaryIndex);
      invertedIndexCreatorMap.get(column).add(dictionaryIndex, docIdCounter);
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
      invertedIndexCreatorMap.get(column).seal();
    }
    writeMetadata();
  }

  void writeMetadata() throws ConfigurationException {
    final PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(file, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_NAME, segmentName);
    properties.setProperty(RESOURCE_NAME, config.getResourceName());
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
    }

    for (final String key : config.getAllCustomKeyValuePair().keySet()) {
      properties.setProperty(key, config.getAllCustomKeyValuePair().get(key));
    }

    if (config.containsKey(TIME_UNIT)) {
      properties.setProperty(TIME_UNIT, config.getString(TIME_UNIT));
    }

    if (config.containsKey(SEGMENT_START_TIME)) {
      properties.setProperty(SEGMENT_START_TIME, config.getString(SEGMENT_START_TIME));
    }

    if (config.containsKey(SEGMENT_END_TIME)) {
      properties.setProperty(SEGMENT_END_TIME, config.getString(SEGMENT_END_TIME));
    }

    for (final String column : indexCreationInfoMap.keySet()) {
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, CARDINALITY),
          String.valueOf(indexCreationInfoMap.get(column).getSortedUniqueElementsArray().length));
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, TOTAL_DOCS),
          String.valueOf(totalDocs));
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, DATA_TYPE),
          schema.getFieldSpecFor(column).getDataType().toString());
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, BITS_PER_ELEMENT), String
              .valueOf(SegmentForwardIndexCreatorImpl
                      .getNumOfBits(indexCreationInfoMap.get(column).getSortedUniqueElementsArray().length)));

      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, DICTIONARY_ELEMENT_SIZE),
          String.valueOf(dictionaryCreatorMap.get(column).getStringColumnMaxLength()));

      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, COLUMN_TYPE),
          String.valueOf(schema.getFieldSpecFor(column).getFieldType().toString()));

      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, IS_SORTED),
          String.valueOf(indexCreationInfoMap.get(column).isSorted()));

      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, HAS_NULL_VALUE),
          String.valueOf(indexCreationInfoMap.get(column).hasNulls()));
      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_DICTIONARY),
          String.valueOf(indexCreationInfoMap.get(column).isCreateDictionary()));

      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, HAS_INVERTED_INDEX),
          String.valueOf(true));

      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, IS_SINGLE_VALUED),
          String.valueOf(schema.getFieldSpecFor(column).isSingleValueField()));

      properties.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, MAX_MULTI_VALUE_ELEMTS),
          String.valueOf(indexCreationInfoMap.get(column).getMaxNumberOfMutiValueElements()));

    }

    properties.save();
  }

}
