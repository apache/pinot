package com.linkedin.pinot.segments.generator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.raw.record.readers.FileFormat;


/**
 * Jun 28, 2014
 *
 * @author Dhaval Patel <dpatel@linkedin.com>
 * 
 */
public class SegmentGeneratorConfiguration extends PropertiesConfiguration {

  private static String SEGMENT_INDEX_VERSION = "segment.index.version";
  private static final String DATA_INPUT_FORMAT = "data.input.format";
  private static final String DATA_INPUT_FILE_PATH = "data.input.file.path";
  private static final String INDEX_OUTPUT_DIR = "index.output.dir";

  private static String DATA_SCHEMA_PROJECTED_COLUMN = "data.schema.projected.column";
  private static String DATA_SCHEMA = "data.schema";
  private static String DELIMETER = "delimeter";
  private static String FIELD_TYPE = "field.type";

  private static final String IS_SINGLE_VALUED_FIELD = "isSingleValued";
  private static final String FIELD_DATA_TYPE = "dataType";

  private static String COMMA = ",";
  private static String DOT = ".";

  public SegmentGeneratorConfiguration() {
    super();
  }

  public SegmentGeneratorConfiguration(File file) throws ConfigurationException {
    super(file);
  }

  public void setOutputDir(String dir) {
    addProperty(INDEX_OUTPUT_DIR, dir);
  }

  public String getOutputDir() {
    return getString(INDEX_OUTPUT_DIR);
  }

  public void setSegmentVersion(SegmentVersion segmentVersion) {
    addProperty(SEGMENT_INDEX_VERSION, segmentVersion.toString());
  }

  public SegmentVersion getSegmentVersion() {
    return SegmentVersion.valueOf(getString(SEGMENT_INDEX_VERSION));
  }

  public FileFormat getFileFormat() {
    return FileFormat.valueOf(getString(DATA_INPUT_FORMAT));
  }

  public void setFileFormat(FileFormat format) {
    addProperty(DATA_INPUT_FORMAT, format.toString());
  }

  public String getFilePath() {
    return getString(DATA_INPUT_FILE_PATH);
  }

  public void setFilePath(String path) {
    addProperty(DATA_INPUT_FILE_PATH, path);
  }

  @SuppressWarnings("unchecked")
  public List<String> getProjectedColumns() {
    return (List<String>) this.getList(DATA_SCHEMA_PROJECTED_COLUMN, new ArrayList<String>());
  }

  public void setProjectedColumns(String[] columns) {
    addProperty(DATA_SCHEMA_PROJECTED_COLUMN, StringUtils.join(columns, ','));
  }

  public void setProjectedColumns(List<String> columns) {
    addProperty(DATA_SCHEMA_PROJECTED_COLUMN, StringUtils.join(columns, ','));
  }

  public Schema getSchema() {
    Schema schema = new Schema();
    List<String> columns = getProjectedColumns();
    for (String column : columns) {
      FieldSpec fieldSpec = new FieldSpec();
      fieldSpec.setName(column);
      fieldSpec.setFieldType(FieldType.valueOf(getString(DATA_SCHEMA + DOT + column + DOT + FIELD_TYPE,
          FieldType.unknown.toString())));
      fieldSpec.setDelimeter(getString(DATA_SCHEMA + DOT + column + DOT + DELIMETER, COMMA));
      schema.addSchema(column, fieldSpec);
    }
    return schema;
  }

  public void setSchema(Schema schema) {
    Collection<FieldSpec> fields = schema.getAllFieldSpecs();
    for (FieldSpec field : fields) {
      addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + FIELD_TYPE, field.getFieldType().toString());
      addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + DELIMETER, field.getDelimeter());
      addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + FIELD_DATA_TYPE, field.getDataType().toString());
      addProperty(DATA_SCHEMA + DOT + field.getName() + DOT + IS_SINGLE_VALUED_FIELD, field.isSingleValueField());
    }
  }
}
