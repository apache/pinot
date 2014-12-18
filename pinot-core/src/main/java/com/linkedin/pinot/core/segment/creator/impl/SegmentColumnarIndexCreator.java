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


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 9, 2014
 */

public class SegmentColumnarIndexCreator implements SegmentCreator {
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

    if (file.exists()) {
      throw new RuntimeException(file.getAbsolutePath());
    }

    file.mkdir();

    this.schema = schema;

    this.totalDocs = totalDocs;

    for (final FieldSpec spec : schema.getAllFieldSpecs()) {
      final ColumnIndexCreationInfo info = indexCreationInfoMap.get(spec.getName());
      dictionaryCreatorMap
          .put(spec.getName(), new SegmentDictionaryCreator(info.hasNulls(), info.getSortedUniqueElementsArray(), spec,
              file));
    }

    for (final String column : dictionaryCreatorMap.keySet()) {
      dictionaryCreatorMap.get(column).build();
    }

    for (final String column : dictionaryCreatorMap.keySet()) {
      forwardIndexCreatorMap.put(column,
          new SegmentForwardIndexCreatorImpl(schema.getFieldSpecFor(column), file, indexCreationInfoMap.get(column)
              .getSortedUniqueElementsArray().length, totalDocs, indexCreationInfoMap.get(column)
              .getTotalNumberOfEntries()));
      invertedIndexCreatorMap.put(column, new SegmentInvertedIndexCreatorImpl(file, indexCreationInfoMap.get(column)
          .getSortedUniqueElementsArray().length, schema.getFieldSpecFor(column)));
    }
  }

  public void index(GenericRow row) {
    for (final String column : dictionaryCreatorMap.keySet()) {
      forwardIndexCreatorMap.get(column).index(dictionaryCreatorMap.get(column).indexOf(row.getValue(column)));
      invertedIndexCreatorMap.get(column).add(dictionaryCreatorMap.get(column).indexOf(row.getValue(column)),
          docIdCounter);
    }
    docIdCounter++;
  }

  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

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

    properties.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_NAME, segmentName);
    properties.addProperty(V1Constants.MetadataKeys.Segment.RESOURCE_NAME, config.getResourceName());
    properties.addProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME, config.getTableName());
    properties.addProperty(V1Constants.MetadataKeys.Segment.DIMENSIONS, config.getDimensions());
    properties.addProperty(V1Constants.MetadataKeys.Segment.METRICS, config.getMetrics());
    properties.addProperty(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.addProperty(V1Constants.MetadataKeys.Segment.TIME_INTERVAL, "not_there");
    properties.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT, config.getTimeUnitForSegment().toString());
    properties.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS, String.valueOf(totalDocs));
    String timeColumn = config.getTimeColumnName();
    properties.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME, indexCreationInfoMap.get(timeColumn).getMin());
    properties.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME, indexCreationInfoMap.get(timeColumn).getMax());
    for (final String key : config.getAllCustomKeyValuePair().keySet()) {
      properties.addProperty(key, config.getAllCustomKeyValuePair().get(key));
    }

    for (final String column : indexCreationInfoMap.keySet()) {
      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.CARDINALITY),
          String.valueOf(indexCreationInfoMap.get(column).getSortedUniqueElementsArray().length));
      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
          String.valueOf(totalDocs));
      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DATA_TYPE), schema
              .getFieldSpecFor(column).getDataType().toString());
      properties
          .addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
              V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT), String
              .valueOf(SegmentForwardIndexCreatorImpl.getNumOfBits(indexCreationInfoMap.get(column)
                  .getSortedUniqueElementsArray().length)));

      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE),
          String.valueOf(dictionaryCreatorMap.get(column).getStringColumnMaxLength()));

      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
          String.valueOf(schema.getFieldSpecFor(column).getFieldType().toString()));

      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SORTED),
          String.valueOf(indexCreationInfoMap.get(column).isSorted()));

      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX),
          String.valueOf(true));
      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED),
          String.valueOf(schema.getFieldSpecFor(column).isSingleValueField()));

      properties.addProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.MAX_MULTI_VALUE_ELEMTS),
          String.valueOf(indexCreationInfoMap.get(column).getMaxNumberOfMutiValueElements()));
    }

    properties.save();
  }

}
