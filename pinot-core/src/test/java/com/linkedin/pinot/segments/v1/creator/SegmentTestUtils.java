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
package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;


public class SegmentTestUtils {

  public static SegmentGeneratorConfig getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro, File outputDir,
      String timeColumn, TimeUnit timeUnit, String tableName) throws FileNotFoundException,
      IOException {
    final SegmentGeneratorConfig segmentGenSpec =
        new SegmentGeneratorConfig(extractSchemaFromAvroWithoutTime(inputAvro));
    segmentGenSpec.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenSpec.setTimeColumnName(timeColumn);
    segmentGenSpec.setTimeUnitForSegment(timeUnit);
    segmentGenSpec.setInputFileFormat(FileFormat.AVRO);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setTableName(tableName);
    segmentGenSpec.setIndexOutputDir(outputDir.getAbsolutePath());
    return segmentGenSpec;
  }

  public static List<String> getColumnNamesFromAvro(File avro) throws FileNotFoundException, IOException {
    List<String> ret = new ArrayList<String>();
    DataFileStream<GenericRecord> dataStream =
        new DataFileStream<GenericRecord>(new FileInputStream(avro), new GenericDatumReader<GenericRecord>());
    for (final Field field : dataStream.getSchema().getFields()) {
      ret.add(field.name());
    }
    return ret;
  }

  public static Schema extractSchemaFromAvro(File avroFile, Map<String, FieldType> fieldTypeMap, TimeUnit granularity)
      throws FileNotFoundException, IOException {
    DataFileStream<GenericRecord> dataStream =
        new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
    Schema schema = new Schema();

    for (final Field field : dataStream.getSchema().getFields()) {
      final String columnName = field.name();
      if (fieldTypeMap.get(field.name()) == FieldType.TIME) {
        final TimeGranularitySpec gSpec =
            new TimeGranularitySpec(getColumnType(dataStream.getSchema().getField(columnName)), granularity, field.name());
        final TimeFieldSpec fSpec = new TimeFieldSpec(gSpec);
        schema.addSchema(columnName, fSpec);
      } else if (fieldTypeMap.get(field.name()) == FieldType.DIMENSION) {
        final FieldSpec fieldSpec = new DimensionFieldSpec();
        fieldSpec.setName(columnName);
        fieldSpec.setFieldType(fieldTypeMap.get(field.name()));
        fieldSpec.setDataType(getColumnType(dataStream.getSchema().getField(columnName)));
        fieldSpec.setSingleValueField(isSingleValueField(dataStream.getSchema().getField(columnName)));
        fieldSpec.setDelimiter(",");
        schema.addSchema(columnName, fieldSpec);
      } else {
        final FieldSpec fieldSpec = new MetricFieldSpec();
        fieldSpec.setName(columnName);
        fieldSpec.setFieldType(fieldTypeMap.get(field.name()));
        fieldSpec.setDataType(getColumnType(dataStream.getSchema().getField(columnName)));
        fieldSpec.setSingleValueField(isSingleValueField(dataStream.getSchema().getField(columnName)));
        fieldSpec.setDelimiter(",");
        schema.addSchema(columnName, fieldSpec);
      }
    }

    dataStream.close();
    return schema;
  }

  public static Schema extractSchemaFromAvroWithoutTime(File avroFile) throws FileNotFoundException, IOException {
    DataFileStream<GenericRecord> dataStream =
        new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
    Schema schema = new Schema();

    for (final Field field : dataStream.getSchema().getFields()) {
      final String columnName = field.name();
      final String pinotType = field.getProp("pinotType");

      final FieldSpec fieldSpec;
      if (pinotType != null && "METRIC".equals(pinotType)) {
        fieldSpec = new MetricFieldSpec();
        fieldSpec.setFieldType(FieldType.METRIC);
      } else {
        fieldSpec = new DimensionFieldSpec();
        fieldSpec.setFieldType(FieldType.DIMENSION); // default
      }

      fieldSpec.setName(columnName);
      fieldSpec.setDataType(getColumnType(dataStream.getSchema().getField(columnName)));
      fieldSpec.setSingleValueField(isSingleValueField(dataStream.getSchema().getField(columnName)));
      fieldSpec.setDelimiter(",");
      schema.addSchema(columnName, fieldSpec);
    }

    dataStream.close();
    return schema;
  }

  private static boolean isSingleValueField(Field field) {
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

  private static Object[] transformAvroArrayToObjectArray(Array arr) {
    if (arr == null) {
      return new Object[0];
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
      ret[i++] = value;
    }
    return ret;
  }
}
