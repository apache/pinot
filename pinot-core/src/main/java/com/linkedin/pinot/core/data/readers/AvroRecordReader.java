/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroRecordReader extends BaseRecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordReader.class);

  private static final String COMMA = ",";

  private String _fileName = null;
  private DataFileStream<GenericRecord> _dataStream = null;
  private FieldExtractor _schemaExtractor = null;
  private GenericRecord _genericRecord = null;

  public AvroRecordReader(final FieldExtractor fieldExtractor, String filePath) throws Exception {
    super();
    _schemaExtractor = fieldExtractor;
    _fileName = filePath;
    super.initNullCounters(_schemaExtractor.getSchema());
    init();
    validateSchema(_schemaExtractor.getSchema());
  }

  @Override
  public void init() throws Exception {
    final File file = new File(_fileName);
    if (!file.exists()) {
      throw new FileNotFoundException("File is not existed!");
    }
    //_schemaExtractor = FieldExtractorFactory.get(_dataReaderSpec);
    if (_fileName.endsWith("gz")) {
      _dataStream =
          new DataFileStream<GenericRecord>(new GZIPInputStream(new FileInputStream(file)),
              new GenericDatumReader<GenericRecord>());
    } else {
      _dataStream =
          new DataFileStream<GenericRecord>(new FileInputStream(file), new GenericDatumReader<GenericRecord>());
    }
  }

  @Override
  public boolean hasNext() {
    return _dataStream.hasNext();
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow row) {
    try {
      _genericRecord = _dataStream.next(_genericRecord);
      return getGenericRow(_genericRecord, row);
    } catch (IOException e) {
      LOGGER.error("Caught exception while reading record", e);
      Utils.rethrowException(e);
      return null;
    }
  }

  private GenericRow getGenericRow(GenericRecord rawRecord, GenericRow row) {
    for (final Field field : _dataStream.getSchema().getFields()) {
      FieldSpec spec = _schemaExtractor.getSchema().getFieldSpecFor(field.name());
      if (spec == null) {
        continue;
      }
      Object value = rawRecord.get(field.name());
      if (value == null) {
        incrementNullCountFor(field.name());
        if (spec.isSingleValueField()) {
          value = spec.getDefaultNullValue();
        } else {
          value = transformAvroArrayToObjectArray((Array) value, spec);
        }
      } else {
        if (value instanceof Utf8) {
          value = ((Utf8) value).toString();
        }
        if (value instanceof Array) {
          value = transformAvroArrayToObjectArray((Array) value, spec);
        }
      }

      row.putField(field.name(), value);
    }

    return row;
  }

  public static Object getDefaultNullValue(FieldSpec spec) {
    Object defaultNullValue = null;
    try {
      defaultNullValue = spec.getDefaultNullValue();
    } catch (UnsupportedOperationException e) {
      // Ignore the exception, and return null to keep the semantics of the call
    }
    return defaultNullValue;
  }

  @Override
  public void close() throws IOException {
    _dataStream.close();
  }

  @Override
  public Schema getSchema() {
    return _schemaExtractor.getSchema();
  }

  private void validateSchema(Schema schema) {
    for (final FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Field fieldStream = _dataStream.getSchema().getField(fieldSpec.getName());
        if (fieldStream == null) {
          LOGGER.warn("Pinot field {} absent in Avro Schema", fieldSpec.getName());
        } else if (fieldSpec.getDataType() != getColumnType(fieldStream)) {
          LOGGER.warn("Pinot field {} of type {} mismatches with corresponding field in Avro Schema of type {}",
              fieldSpec.getName(), fieldSpec.getDataType(), getColumnType(fieldStream));
        }
        else if (fieldSpec.isSingleValueField() != isSingleValueField(fieldStream)) {
          LOGGER.warn("{} -valued Pinot field {} mismatches with corresponding {} -valued field in Avro Schema",
              fieldSpec.isSingleValueField() ? "Single" : "Multi", fieldSpec.getName(),
              isSingleValueField(fieldStream)? "Single" : "Multi");
      }
    }
  }

  public static boolean isSingleValueField(Field field) {
    org.apache.avro.Schema fieldSchema = field.schema();
    fieldSchema = extractSchemaFromUnionIfNeeded(fieldSchema);

    final Type type = fieldSchema.getType();
    if (type == Type.ARRAY) {
      return false;
    }
    return true;
  }

  public static DataType getColumnType(Field field) {
    org.apache.avro.Schema fieldSchema = field.schema();
    fieldSchema = extractSchemaFromUnionIfNeeded(fieldSchema);

    final Type type = fieldSchema.getType();
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

  public static Object[] transformAvroArrayToObjectArray(Array arr, FieldSpec spec) {
    if (arr == null) {
      return new Object[] { getDefaultNullValue(spec) };
    }
    if (arr.size() == 0) {
      return new Object[] { getDefaultNullValue(spec) };
    }

    final Object[] ret = new Object[arr.size()];
    final Iterator iterator = arr.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Object value = iterator.next();
      if (value instanceof Record) {
        value = ((Record) value).get(0);
      }
      if (value instanceof Utf8) {
        value = ((Utf8) value).toString();
      }
      if (value == null) {
        value = getDefaultNullValue(spec);
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
