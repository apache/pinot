package com.linkedin.pinot.core.chunk.creator.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.chunk.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 9, 2014
 */

public class ChunkColumnarIndexCreator implements SegmentCreator {
  private ChunkGeneratorConfiguration config;
  private Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  private Map<String, ChunkDictionaryCreator> dictionaryCreatorMap;
  private Map<String, ChunkForwardIndexCreatorImpl> forwardIndexCreatorMap;
  private Map<String, InvertedIndexCreator> invertedIndexCreatorMap;
  private String segmentName;

  private Schema schema;
  private File file;
  private int totalDocs;
  private int docIdCounter;

  @Override
  public void init(ChunkGeneratorConfiguration segmentCreationSpec) {
    config = segmentCreationSpec;
  }

  public void init(ChunkGeneratorConfiguration segmentCreationSpec, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap,
      Schema schema, int totalDocs, File outDir) throws Exception {
    docIdCounter = 0;
    config = segmentCreationSpec;
    this.indexCreationInfoMap = indexCreationInfoMap;
    dictionaryCreatorMap = new HashMap<String, ChunkDictionaryCreator>();
    forwardIndexCreatorMap = new HashMap<String, ChunkForwardIndexCreatorImpl>();
    this.indexCreationInfoMap = indexCreationInfoMap;
    invertedIndexCreatorMap = new HashMap<String, InvertedIndexCreator>();
    file = outDir;

    if (file.exists()) {
      throw new FileAlreadyExistsException(file.getAbsolutePath());
    }
    file.mkdir();

    this.schema = schema;

    this.totalDocs = totalDocs;

    for (final FieldSpec spec : schema.getAllFieldSpecs()) {
      final ColumnIndexCreationInfo info = indexCreationInfoMap.get(spec.getName());
      dictionaryCreatorMap
      .put(spec.getName(), new ChunkDictionaryCreator(info.hasNulls(), info.getSortedUniqueElementsArray(), spec, file));
    }

    for (final String column : dictionaryCreatorMap.keySet()) {
      dictionaryCreatorMap.get(column).build();
    }

    for (final String column : dictionaryCreatorMap.keySet()) {
      forwardIndexCreatorMap.put(column,
          new ChunkForwardIndexCreatorImpl(schema.getFieldSpecFor(column), file, indexCreationInfoMap.get(column)
              .getSortedUniqueElementsArray().length, totalDocs, indexCreationInfoMap.get(column).getTotalNumberOfEntries()));
      invertedIndexCreatorMap.put(column, new ChunkInvertedIndexCreatorImpl(file, indexCreationInfoMap.get(column)
          .getSortedUniqueElementsArray().length, schema.getFieldSpecFor(column)));
    }
  }

  public void index(GenericRow row) {
    for (final String column : dictionaryCreatorMap.keySet()) {
      forwardIndexCreatorMap.get(column).index(dictionaryCreatorMap.get(column).indexOf(row.getValue(column)));
      invertedIndexCreatorMap.get(column).add(dictionaryCreatorMap.get(column).indexOf(row.getValue(column)), docIdCounter);
    }
    docIdCounter++;
  }

  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  public void seal() throws ConfigurationException, IOException {
    System.out.println("docIdCounter is : " + docIdCounter);
    for (final String column : forwardIndexCreatorMap.keySet()) {
      forwardIndexCreatorMap.get(column).close();
      invertedIndexCreatorMap.get(column).seal();
    }
    writeMetadata();
  }

  void writeMetadata() throws ConfigurationException {
    final PropertiesConfiguration properties = new PropertiesConfiguration(new File(file, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_NAME, segmentName);
    properties.addProperty(V1Constants.MetadataKeys.Segment.RESOURCE_NAME, config.getResourceName());
    properties.addProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME, config.getTableName());
    properties.addProperty(V1Constants.MetadataKeys.Segment.DIMENSIONS, config.getDimensions());
    properties.addProperty(V1Constants.MetadataKeys.Segment.METRICS, config.getMetrics());
    properties.addProperty(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.addProperty(V1Constants.MetadataKeys.Segment.TIME_INTERVAL, "not_there");
    properties.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT, config.getTimeUnitForSegment().toString());
    properties.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS, String.valueOf(totalDocs));

    for (final String key : config.getAllCustomKeyValuePair().keySet()) {
      properties.addProperty(key, config.getAllCustomKeyValuePair().get(key));
    }

    for (final String column : indexCreationInfoMap.keySet()) {
      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.CARDINALITY),
          String.valueOf(indexCreationInfoMap.get(column).getSortedUniqueElementsArray().length));
      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
          String.valueOf(totalDocs));
      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DATA_TYPE), schema
          .getFieldSpecFor(column).getDataType().toString());
      properties
      .addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT), String
          .valueOf(ChunkForwardIndexCreatorImpl.getNumOfBits(indexCreationInfoMap.get(column).getSortedUniqueElementsArray().length)));

      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE),
          String.valueOf(dictionaryCreatorMap.get(column).getStringColumnMaxLength()));

      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
          String.valueOf(schema.getFieldSpecFor(column).getFieldType().toString()));

      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SORTED),
          String.valueOf(indexCreationInfoMap.get(column).isSorted()));

      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX),
          String.valueOf(true));
      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED),
          String.valueOf(schema.getFieldSpecFor(column).isSingleValueField()));

      properties.addProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.MAX_MULTI_VALUE_ELEMTS),
          String.valueOf(indexCreationInfoMap.get(column).getMaxNumberOfMutiValueElements()));
    }

    properties.save();
  }

  @Override
  public IndexSegment buildSegment() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

}
