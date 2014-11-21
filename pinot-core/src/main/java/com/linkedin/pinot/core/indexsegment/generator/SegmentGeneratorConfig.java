package com.linkedin.pinot.core.indexsegment.generator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.chunk.creator.impl.V1Constants.MetadataKeys;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.utils.Helpers.STRING;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


/**
 * Jun 28, 2014
 *
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class SegmentGeneratorConfig extends PropertiesConfiguration {

  private static String SEGMENT_INDEX_VERSION = "segment.index.version";
  private static String SEGMENT_TIME_COLUMN_NAME = "segment.time.column.name";

  private static final String DATA_INPUT_FORMAT = "data.input.format";
  private static final String DATA_INPUT_FILE_PATH = "data.input.file.path";
  private static final String INDEX_OUTPUT_DIR = "index.output.dir";
  private static String DATA_SCHEMA_PROJECTED_COLUMN = "data.schema.projected.column";
  private static String DATA_SCHEMA = "data.schema";
  private static String DELIMETER = "delimeter";
  private static String FIELD_TYPE = "field.type";
  private static final String IS_SINGLE_VALUED_FIELD = "isSingleValued";
  private static final String FIELD_DATA_TYPE = "dataType";
  private static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";

  private static String COMMA = ",";
  private static String DOT = ".";

  /*
   *
   * Segment metadata, needed properties to sucessfull create the segment
   *
   * */

  public void setSegmentNamePostfix(String prefix) {
    addProperty(SEGMENT_NAME_POSTFIX, prefix);
  }

  public String getSegmentNamePostfix() {
    return getString(SEGMENT_NAME_POSTFIX);
  }


  public void setResourceName(String resourceName) {
    addProperty(MetadataKeys.Segment.RESOURCE_NAME, resourceName);
  }

  public String getResourceName() {
    return getString(MetadataKeys.Segment.RESOURCE_NAME);
  }

  public void setTableName(String tableName) {
    addProperty(MetadataKeys.Segment.TABLE_NAME, tableName);
  }

  public String getTableName() {
    return getString(MetadataKeys.Segment.TABLE_NAME);
  }

  public String getDimensions() {
    return getQualifyingDimensions(FieldType.dimension);
  }

  public String getMetrics() {
    return getQualifyingDimensions(FieldType.metric);
  }

  public void setTimeColumnName(String name) {
    addProperty(SEGMENT_TIME_COLUMN_NAME, name);
  }

  public String getTimeColumnName() {
    if (containsKey(SEGMENT_TIME_COLUMN_NAME)) {
      return getString(SEGMENT_TIME_COLUMN_NAME);
    }
    return getQualifyingDimensions(FieldType.time);
  }

  public void setTimeUnitForSegment(SegmentTimeUnit timeUnit) {
    addProperty(MetadataKeys.Segment.TIME_UNIT, timeUnit.toString());
  }

  public SegmentTimeUnit getTimeUnitForSegment() {
    return SegmentTimeUnit.valueOf(getString(MetadataKeys.Segment.TIME_UNIT));
  }

  public void setCustom(String key, String value) {
    addProperty(STRING.concat(',', MetadataKeys.Segment.CUSTOM_PROPERTIES_PREFIX, key), value);
  }

  public Map<String, String> getAllCustomKeyValuePair() {
    final Map<String, String> customConfigs = new HashMap<String, String>();
    final Iterator<String> allKeys = getKeys();
    while (allKeys.hasNext()) {
      final String key = allKeys.next();
      if (key.startsWith(MetadataKeys.Segment.CUSTOM_PROPERTIES_PREFIX)) {
        customConfigs.put(key, getProperty(key).toString());
      }
    }
    return customConfigs;
  }

  private String getQualifyingDimensions(FieldType type) {
    String dims = "";
    for (final FieldSpec spec : getSchema().getAllFieldSpecs()) {
      if (spec.getFieldType() == type) {
        dims += spec.getName() + ",";
      }
    }
    return StringUtils.chomp(dims, ",");
  }

  public SegmentGeneratorConfig() {
    super();
  }

  public SegmentGeneratorConfig(File file) throws ConfigurationException {
    super(file);
  }

  public void setIndexOutputDir(String dir) {
    addProperty(INDEX_OUTPUT_DIR, dir);
  }

  public String getIndexOutputDir() {
    return getString(INDEX_OUTPUT_DIR);
  }

  public void setSegmentVersion(SegmentVersion segmentVersion) {
    addProperty(SEGMENT_INDEX_VERSION, segmentVersion.toString());
  }

  public SegmentVersion getSegmentVersion() {
    return SegmentVersion.valueOf(getString(SEGMENT_INDEX_VERSION));
  }

  public FileFormat getInputFileFormat() {
    return FileFormat.valueOf(getString(DATA_INPUT_FORMAT));
  }

  public void setInputFileFormat(FileFormat format) {
    addProperty(DATA_INPUT_FORMAT, format.toString());
  }

  public String getInputFilePath() {
    return getString(DATA_INPUT_FILE_PATH);
  }

  public void setInputFilePath(String path) {
    addProperty(DATA_INPUT_FILE_PATH, path);
  }

  @SuppressWarnings("unchecked")
  public List<String> getProjectedColumns() {
    return this.getList(DATA_SCHEMA_PROJECTED_COLUMN, new ArrayList<String>());
  }

  public void setProjectedColumns(String[] columns) {
    addProperty(DATA_SCHEMA_PROJECTED_COLUMN, StringUtils.join(columns, ','));
  }

  public void setProjectedColumns(List<String> columns) {
    addProperty(DATA_SCHEMA_PROJECTED_COLUMN, StringUtils.join(columns, ','));
  }

  public Schema getSchema() {
    final Schema schema = new Schema();
    final List<String> columns = getProjectedColumns();
    for (final String column : columns) {
      final FieldSpec fieldSpec = new FieldSpec();
      fieldSpec.setName(column);
      fieldSpec.setFieldType(FieldType.valueOf(getString(DATA_SCHEMA + DOT + column + DOT + FIELD_TYPE,
          FieldType.unknown.toString())));
      fieldSpec.setDelimeter(getString(DATA_SCHEMA + DOT + column + DOT + DELIMETER, COMMA));
      schema.addSchema(column, fieldSpec);
    }
    return schema;
  }

  public void setSchema(Schema schema) {
    final Collection<FieldSpec> fields = schema.getAllFieldSpecs();
    for (final FieldSpec field : fields) {
      addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + FIELD_TYPE, field.getFieldType().toString());
      addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + DELIMETER, field.getDelimeter());
      if (field.getDataType() != null) {
        addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + FIELD_DATA_TYPE, field.getDataType().toString());
      }
      addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + IS_SINGLE_VALUED_FIELD, field.isSingleValueField());
    }
  }
}
