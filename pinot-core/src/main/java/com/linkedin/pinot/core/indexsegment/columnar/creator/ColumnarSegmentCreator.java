package com.linkedin.pinot.core.indexsegment.columnar.creator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.pinot.core.data.FieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.Schema;
import com.linkedin.pinot.core.data.readers.FileSystemMode;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreator;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.OffHeapCompressedIntArray;


public class ColumnarSegmentCreator implements SegmentCreator {

  public static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ColumnarSegmentCreator.class);

  private SegmentGeneratorConfiguration config;
  private RecordReader reader;

  private Map<String, DictionaryCreator> dictionaryCreatorsMap;
  private Map<String, Dictionary<?>> dictionaryMap;
  private Map<String, IndexCreator> indexCreatorMap;
  private Schema dataSchema;
  private File indexDir;
  private SegmentVersion v;

  public ColumnarSegmentCreator() {

  }

  public ColumnarSegmentCreator(SegmentVersion version, RecordReader recordReader1) {
    this.reader = recordReader1;
    this.dictionaryCreatorsMap = new HashMap<String, DictionaryCreator>();
    this.dictionaryMap = new HashMap<String, Dictionary<?>>();
    this.indexCreatorMap = new HashMap<String, IndexCreator>();
    this.dataSchema = recordReader1.getSchema();
    this.v = version;
  }

  @Override
  public void init(SegmentGeneratorConfiguration segmentCreationSpec) {
    this.config = segmentCreationSpec;
    this.indexDir = new File(config.getIndexOutputDir());

    if (this.indexDir.exists()) {
      throw new IllegalStateException("index directory passed in already exists : " + this.indexDir.getAbsolutePath());
    }
    indexDir.mkdir();
  }

  private void logBeforeStats() {
    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("***********************");
      logger.info("found " + spec.getName() + " index of type : " + spec.getDataType());
    }
  }

  private void logAfterStats() {
    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("*****************************");
      logger.info("creation stats for : " + spec.getName());
      logger.info("Cadinality : " + dictionaryCreatorsMap.get(spec.getName()).getDictionarySize());
      logger.info("total number of entries : " + dictionaryCreatorsMap.get(spec.getName()).getTotalDocs());
      logger.info("dictionary overall time : " + dictionaryCreatorsMap.get(spec.getName()).totalTimeTaken());
      logger.info("forward index overall time : " + indexCreatorMap.get(spec.getName()).totalTimeTaken());
    }
  }

  private void initializeDictionaryCreators() {
    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("intializing dictionary creator for : " + spec.getName());
      dictionaryCreatorsMap.put(spec.getName(), new DictionaryCreator(spec, indexDir));
    }
  }

  public DictionaryCreator getDicCrFor(String col) {
    return dictionaryCreatorsMap.get(col);
  }

  private void initializeIndexCreators() throws IOException {
    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("intializing Column Index creator for : " + spec.getName());
      indexCreatorMap.put(spec.getName(), new IndexCreator(indexDir, dictionaryCreatorsMap.get(spec.getName()), spec,
          FileSystemMode.DISK));
    }
  }

  private void createDictionatries() throws IOException {
    logger.info("starting indexing dictionary");
    while (reader.hasNext()) {
      GenericRow row = reader.next();
      for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
        dictionaryCreatorsMap.get(spec.getName()).add(row.getValue(spec.getName()));
      }
    }

    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      logger.info("sealing dictionary for column : " + spec.getName());
      dictionaryMap.put(spec.getName(), dictionaryCreatorsMap.get(spec.getName()).seal());
    }
  }

  private void createIndexes() throws IOException {
    while (reader.hasNext()) {
      GenericRow row = reader.next();
      for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
        Object entry = row.getValue(spec.getName());
        String column = spec.getName();
        int dictionaryId = dictionaryCreatorsMap.get(column).indexOf(entry);
        indexCreatorMap.get(column).add(dictionaryId);
      }
    }

    for (String col : indexCreatorMap.keySet()) {
      logger.info("sealing indexes for column : " + col);
      indexCreatorMap.get(col).seal();
    }
  }

  @Override
  public IndexSegment buildSegment() throws Exception {

    logBeforeStats();

    initializeDictionaryCreators();

    logger.info("rewinding readers");

    createDictionatries();

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
    File versions = new File(indexDir, V1Constants.VERSIONS_FILE);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(versions));
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
    PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    for (java.util.Map.Entry<String, String> entry : getSegmentProperties().entrySet())
      properties.addProperty(entry.getKey(), entry.getValue());

    for (java.util.Map.Entry<String, String> entry : getColumnProperties().entrySet())
      properties.addProperty(entry.getKey(), entry.getValue());

    properties.save();
  }

  public Map<String, String> getSegmentProperties() {
    Map<String, String> properties = new HashMap<String, String>();
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
    Map<String, String> properties = new HashMap<String, String>();

    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      String column = spec.getName();
      DictionaryCreator dictionaryCr = dictionaryCreatorsMap.get(column);
      properties.put(
          V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(), V1Constants.MetadataKeys.Column.CARDINALITY),
          String.valueOf(dictionaryCr.cardinality()));
      properties.put(
          V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(), V1Constants.MetadataKeys.Column.TOTAL_DOCS),
          String.valueOf(dictionaryCr.getTotalDocs()));

      properties.put(V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(),
          V1Constants.MetadataKeys.Column.DATA_TYPE), spec.getDataType().toString());

      properties.put(
          V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(), V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT),
          String.valueOf(OffHeapCompressedIntArray.getNumOfBits(dictionaryCr.cardinality())));

      properties
          .put(V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(),
              V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryCr
              .getLengthOfEachEntry()));

      properties.put(
          V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(), V1Constants.MetadataKeys.Column.COLUMN_TYPE),
          String.valueOf(spec.getFieldType().toString()));

      properties.put(
          V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(), V1Constants.MetadataKeys.Column.IS_SORTED),
          String.valueOf(dictionaryCr.isSorted()));

      // hard coding for now
      properties
          .put(V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(),
              V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX), String.valueOf(false));
      properties.put(
          V1Constants.MetadataKeys.Column.getKeyFor(spec.getName(), V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED),
          String.valueOf(true));
    }

    return properties;
  }

  public void loadSegment() {

  }
}
