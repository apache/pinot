package com.linkedin.pinot.raw.record.readers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.FieldSpec.DataType;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.index.data.GenericRow;
import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.raw.record.extractors.FieldExtractor;
import com.linkedin.pinot.raw.record.extractors.FieldExtractorFactory;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;


public class AvroRecordReader implements RecordReader {
  private static final String COMMA = ",";
  private static final String DATA_INPUT_FILE_PATH = "data.input.file.path";

  private final SegmentGeneratorConfiguration _dataReaderSpec;
  private String _fileName = null;
  private DataFileStream<GenericRecord> _dataStream = null;
  private FieldExtractor _schemaExtractor = null;

  private GenericRow _genericRow = new GenericRow();
  private Map<String, Object> _fieldMap = new HashMap<String, Object>();

  public AvroRecordReader(final SegmentGeneratorConfiguration dataReaderSpec) throws Exception {
    _dataReaderSpec = dataReaderSpec;
    _fileName = _dataReaderSpec.getString(DATA_INPUT_FILE_PATH);
    init();
  }

  @Override
  public void init() throws Exception {
    File file = new File(_fileName);
    if (!file.exists()) {
      throw new FileNotFoundException("File is not existed!");
    }
    _schemaExtractor = FieldExtractorFactory.get(_dataReaderSpec);
    _dataStream = new DataFileStream<GenericRecord>(new FileInputStream(file), new GenericDatumReader<GenericRecord>());
    updateSchema(_schemaExtractor.getSchema());
  }

  @Override
  public boolean hasNext() {
    return _dataStream.hasNext();
  }

  @Override
  public GenericRow next() {
    return _schemaExtractor.transform(getGenericRow(_dataStream.next()));
  }

  private GenericRow getGenericRow(GenericRecord rawRecord) {
    for (Field field : _dataStream.getSchema().getFields()) {
      Object value = rawRecord.get(field.name());
      if (value instanceof Utf8) {
        value = ((Utf8) value).toString();
      }
      if (value instanceof Array) {
        value = transformAvroArrayToObjectArray((Array) value);
      }
      _fieldMap.put(field.name(), value);
    }
    _genericRow.init(_fieldMap);
    return _genericRow;
  }

  @Override
  public void close() throws IOException {
    _dataStream.close();
  }

  @Override
  public Schema getSchema() {
    return _schemaExtractor.getSchema();
  }

  private void updateSchema(Schema schema) {
    if (schema == null || schema.size() == 0) {
      for (Field field : _dataStream.getSchema().getFields()) {
        String columnName = field.name();
        FieldSpec fieldSpec = new FieldSpec();
        fieldSpec.setName(columnName);
        fieldSpec.setFieldType(FieldType.unknown);
        fieldSpec.setDataType(getColumnType(_dataStream.getSchema().getField(columnName)));
        fieldSpec.setSingleValueField(isSingleValueField(_dataStream.getSchema().getField(columnName)));
        fieldSpec.setDelimeter(COMMA);
        schema.addSchema(columnName, fieldSpec);
      }

    } else {
      for (String columnName : schema.getColumnNames()) {
        FieldSpec fieldSpec = new FieldSpec();
        fieldSpec.setName(columnName);
        fieldSpec.setFieldType(schema.getFieldType(columnName));
        fieldSpec.setDelimeter(schema.getDelimeter(columnName));

        fieldSpec.setDataType(getColumnType(_dataStream.getSchema().getField(columnName)));
        fieldSpec.setSingleValueField(isSingleValueField(_dataStream.getSchema().getField(columnName)));
        schema.addSchema(columnName, fieldSpec);
      }
    }
  }

  private static boolean isSingleValueField(Field field) {
    org.apache.avro.Schema fieldSchema = field.schema();
    fieldSchema = extractSchemaFromUnionIfNeeded(fieldSchema);

    Type type = fieldSchema.getType();
    if (type == Type.ARRAY) {
      return false;
    }
    return true;
  }

  private static DataType getColumnType(Field field) {
    org.apache.avro.Schema fieldSchema = field.schema();
    fieldSchema = extractSchemaFromUnionIfNeeded(fieldSchema);

    Type type = fieldSchema.getType();
    if (type == Type.ARRAY) {
      org.apache.avro.Schema elementSchema = extractSchemaFromUnionIfNeeded(fieldSchema.getElementType());
      if (elementSchema.getType() == Type.RECORD) {
        if (elementSchema.getFields().size() == 1) {
          elementSchema = elementSchema.getFields().get(0).schema();
        } else {
          throw new RuntimeException("More than one schema in Multi-value column!");
        }
        elementSchema = extractSchemaFromUnionIfNeeded(elementSchema);
      }
      return DataType.valueOf(elementSchema.getType());
    } else {
      return DataType.valueOf(type);
    }
  }

  private static org.apache.avro.Schema extractSchemaFromUnionIfNeeded(org.apache.avro.Schema fieldSchema) {
    if ((fieldSchema).getType() == Type.UNION) {
      fieldSchema = ((org.apache.avro.Schema) CollectionUtils.find(fieldSchema.getTypes(), new Predicate() {
        @Override
        public boolean evaluate(Object object) {
          return ((org.apache.avro.Schema) object).getType() != Type.NULL;
        }
      }));
    }
    return fieldSchema;
  }

  private static Object[] transformAvroArrayToObjectArray(Array arr) {
    if (arr == null) {
      return new Object[0];
    }
    Object[] ret = new Object[(int) arr.size()];
    Iterator iterator = arr.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Object value = iterator.next();
      if (value instanceof Record) {
        value = ((Record) value).get(0);
      }
      if (value instanceof Utf8) {
        value = ((Utf8) value).toString();
      }
      ret[i++] = value;
    }
    return ret;
  }

  @Override
  public void rewind() throws Exception {
    _dataStream.close();
    _dataStream = null;
    init();
  }

}
