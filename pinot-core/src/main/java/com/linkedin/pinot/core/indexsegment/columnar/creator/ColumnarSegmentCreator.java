package com.linkedin.pinot.core.indexsegment.columnar.creator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.chunk.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileSystemMode;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.OffHeapCompressedIntArray;


public class ColumnarSegmentCreator implements SegmentCreator {

  public static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ColumnarSegmentCreator.class);

  private SegmentGeneratorConfig config;
  private RecordReader reader;

  private Map<String, DictionaryCreator> dictionaryCreatorsMap;
  private Map<String, Dictionary<?>> dictionaryMap;
  private Map<String, IndexCreator> forwardIndexCreatorMap;
  private Map<String, InvertedIndexCreator> invertedIndexCreatorMap;
  private Schema dataSchema;
  private File indexDir;
  private SegmentVersion v;

  public ColumnarSegmentCreator() {

  }

  public ColumnarSegmentCreator(SegmentVersion version, RecordReader recordReader1) {
    reader = recordReader1;
    dictionaryCreatorsMap = new HashMap<String, DictionaryCreator>();
    dictionaryMap = new HashMap<String, Dictionary<?>>();
    forwardIndexCreatorMap = new HashMap<String, IndexCreator>();
    invertedIndexCreatorMap = new HashMap<String, InvertedIndexCreator>();
    dataSchema = recordReader1.getSchema();
    v = version;
  }

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema,
      int totalDocs, File outDir) throws Exception {
    // TODO Auto-generated method stub
    config = segmentCreationSpec;
    indexDir = new File(config.getIndexOutputDir());

    if (indexDir.exists()) {
      throw new IllegalStateException("index directory passed in already exists : " + indexDir.getAbsolutePath());
    }
    indexDir.mkdir();
  }

  private void logBeforeStats() {
    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("found " + spec.getName() + " index of type : " + spec.getDataType());
    }
  }

  private void logAfterStats() {
    for (final String column : dictionaryCreatorsMap.keySet()) {
      logger.info("*****************************");
      logger.info("creation stats for : " + column);
      logger.info("Cadinality : " + dictionaryCreatorsMap.get(column).getDictionarySize());
      logger.info("total number of entries : " + dictionaryCreatorsMap.get(column).getTotalDocs());
      logger.info("dictionary overall time : " + dictionaryCreatorsMap.get(column).totalTimeTaken());
      logger.info("forward index overall time : " + forwardIndexCreatorMap.get(column).totalTimeTaken());
      logger.info("forward index overall time : " + invertedIndexCreatorMap.get(column).totalTimeTakeSoFar());
      logger.info("*****************************");
    }
  }

  private void initializeDictionaryCreators() {
    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("intializing dictionary creator for : " + spec.getName());
      dictionaryCreatorsMap.put(spec.getName(), new DictionaryCreator(spec, indexDir));
    }
  }

  private void initializeIndexCreators() throws IOException {
    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("intializing Column Index creator for : " + spec.getName());
      forwardIndexCreatorMap.put(spec.getName(), new IndexCreator(indexDir, dictionaryCreatorsMap.get(spec.getName()), spec,
          FileSystemMode.DISK));
      invertedIndexCreatorMap
      .put(spec.getName(), new BitmapInvertedIndexCreator(indexDir, dictionaryCreatorsMap.get(spec.getName()), spec));
      // new IntArrayInvertedIndexCreator(indexDir, dictionaryCreatorsMap.get(spec.getName()), spec));

    }
  }

  private void createDictionatries() throws IOException {
    logger.info("starting indexing dictionary");
    while (reader.hasNext()) {
      final GenericRow row = reader.next();
      for (final String column : dictionaryCreatorsMap.keySet()) {
        dictionaryCreatorsMap.get(column).add(row.getValue(column));
      }
    }

    for (final String column : dictionaryCreatorsMap.keySet()) {
      logger.info("sealing dictionary for column : " + column);
      dictionaryMap.put(column, dictionaryCreatorsMap.get(column).seal());
    }
  }

  private void createIndexes() throws IOException {
    int docId = 0;
    while (reader.hasNext()) {
      final GenericRow row = reader.next();
      for (final String column : forwardIndexCreatorMap.keySet()) {
        final Object entry = row.getValue(column);
        final int dictionaryId = dictionaryCreatorsMap.get(column).indexOf(entry);
        forwardIndexCreatorMap.get(column).add(dictionaryId);
        invertedIndexCreatorMap.get(column).add(dictionaryId, docId);
      }
      docId++;
    }

    for (final String col : forwardIndexCreatorMap.keySet()) {
      logger.info("sealing indexes for column : " + col);
      forwardIndexCreatorMap.get(col).seal();
      invertedIndexCreatorMap.get(col).seal();
    }
  }

  @Override
  public IndexSegment buildSegment() throws Exception {

    logBeforeStats();

    initializeDictionaryCreators();

    createDictionatries();

    logger.info("rewinding readers");

    reader.rewind();

    initializeIndexCreators();

    createIndexes();

    logAfterStats();

    reader.close();

    createMetadata();

    versionIt();

    closeClosables();

    return null;
  }

  public void versionIt() throws IOException {
    final File versions = new File(indexDir, V1Constants.VERSIONS_FILE);
    final DataOutputStream out = new DataOutputStream(new FileOutputStream(versions));
    out.write(v.toString().getBytes("UTF8"));
    out.close();

    // how to read
    //    DataInputStream is = new DataInputStream(new FileInputStream(versions));
    //    byte[] vce = new byte["v1".getBytes("UTF8").length];
    //    is.read(vce, 0, "v1".getBytes("UTF8").length);
    //    System.out.println(new String(vce, "UTF8"));
  }

  public void closeClosables() {

  }

  public void createMetadata() throws ConfigurationException {
    final PropertiesConfiguration properties = new PropertiesConfiguration(new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    for (final java.util.Map.Entry<String, String> entry : getSegmentProperties().entrySet()) {
      properties.addProperty(entry.getKey(), entry.getValue());
    }

    for (final java.util.Map.Entry<String, String> entry : getColumnProperties().entrySet()) {
      properties.addProperty(entry.getKey(), entry.getValue());
    }

    properties.save();
  }

  public Map<String, String> getSegmentProperties() {
    final Map<String, String> properties = new HashMap<String, String>();
    properties.put(V1Constants.MetadataKeys.Segment.RESOURCE_NAME, config.getResourceName());
    properties.put(V1Constants.MetadataKeys.Segment.TABLE_NAME, config.getTableName());
    properties.put(V1Constants.MetadataKeys.Segment.DIMENSIONS, config.getDimensions());
    properties.put(V1Constants.MetadataKeys.Segment.METRICS, config.getMetrics());
    properties.put(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.put(V1Constants.MetadataKeys.Segment.TIME_INTERVAL, "nullForNow");
    properties.put(V1Constants.MetadataKeys.Segment.TIME_UNIT, config.getTimeUnitForSegment().toString());
    properties.put(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS,
        String.valueOf(dictionaryCreatorsMap.entrySet().iterator().next().getValue().getTotalDocs()));
    properties.putAll(config.getAllCustomKeyValuePair());
    return properties;
  }

  public Map<String, String> getColumnProperties() {
    final Map<String, String> properties = new HashMap<String, String>();

    for (final String column : dictionaryCreatorsMap.keySet()) {
      final DictionaryCreator dictionaryCr = dictionaryCreatorsMap.get(column);
      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.CARDINALITY),
          String.valueOf(dictionaryCr.cardinality()));
      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
          String.valueOf(dictionaryCr.getTotalDocs()));

      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DATA_TYPE), dataSchema
          .getFieldSpecFor(column).getDataType().toString());

      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT),
          String.valueOf(OffHeapCompressedIntArray.getNumOfBits(dictionaryCr.cardinality())));

      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE),
          String.valueOf(dictionaryCr.getLengthOfEachEntry()));

      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
          String.valueOf(dataSchema.getFieldSpecFor(column).getFieldType().toString()));

      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SORTED),
          String.valueOf(dictionaryCr.isSorted()));

      // hard coding for now
      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX),
          String.valueOf(true));

      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED),
          String.valueOf(true));
    }

    return properties;
  }

  public void loadSegment() {

  }

}
