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
package com.linkedin.pinot.segments.v1.creator;

import com.google.common.base.Preconditions;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
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


public class SegmentTestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentTestUtils.class);

  @Nonnull
  public static SegmentGeneratorConfig getSegmentGeneratorConfigWithoutTimeColumn(@Nonnull File avroFile,
      @Nonnull File outputDir, @Nonnull String tableName) throws IOException {
    SegmentGeneratorConfig segmentGeneratorConfig =
        new SegmentGeneratorConfig(extractSchemaFromAvroWithoutTime(avroFile));
    segmentGeneratorConfig.setInputFilePath(avroFile.getAbsolutePath());
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
    segmentGeneratorConfig.setTableName(tableName);
    return segmentGeneratorConfig;
  }

  public static SegmentGeneratorConfig getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro, File outputDir,
      String timeColumn, TimeUnit timeUnit, String tableName) throws IOException {
    final SegmentGeneratorConfig segmentGenSpec =
        new SegmentGeneratorConfig(extractSchemaFromAvroWithoutTime(inputAvro));
    segmentGenSpec.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenSpec.setTimeColumnName(timeColumn);
    segmentGenSpec.setSegmentTimeUnit(timeUnit);
    segmentGenSpec.setFormat(FileFormat.AVRO);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setTableName(tableName);
    segmentGenSpec.setOutDir(outputDir.getAbsolutePath());
    segmentGenSpec.createInvertedIndexForAllColumns();
    return segmentGenSpec;
  }

  public static SegmentGeneratorConfig getSegmentGeneratorConfigWithSchema(File inputAvro, File outputDir,
      String tableName, Schema schema) {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
    segmentGeneratorConfig.setFormat(FileFormat.AVRO);
    segmentGeneratorConfig.setSegmentVersion(SegmentVersion.v1);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setTimeColumnName(schema.getTimeColumnName());
    segmentGeneratorConfig.setSegmentTimeUnit(schema.getOutgoingTimeUnit());
    return segmentGeneratorConfig;
  }

  public static List<String> getColumnNamesFromAvro(File avro) throws IOException {
    List<String> ret = new ArrayList<String>();
    DataFileStream<GenericRecord> dataStream =
        new DataFileStream<GenericRecord>(new FileInputStream(avro), new GenericDatumReader<GenericRecord>());
    for (final Field field : dataStream.getSchema().getFields()) {
      ret.add(field.name());
    }
    return ret;
  }

  public static Schema extractSchemaFromAvro(File avroFile, Map<String, FieldType> fieldTypeMap, TimeUnit granularity)
      throws IOException {
    DataFileStream<GenericRecord> dataStream =
        new DataFileStream<>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
    Schema schema = new Schema();

    for (final Field field : dataStream.getSchema().getFields()) {
      final String columnName = field.name();
      FieldType fieldType = fieldTypeMap.get(columnName);
      Preconditions.checkNotNull(fieldType);

      switch (fieldType) {
        case TIME:
          final TimeGranularitySpec gSpec = new TimeGranularitySpec(getColumnType(field), granularity, columnName);
          final TimeFieldSpec fSpec = new TimeFieldSpec(gSpec);
          schema.addField(fSpec);
          continue;
        case DIMENSION:
          final FieldSpec dimensionFieldSpec =
              new DimensionFieldSpec(columnName, getColumnType(field), isSingleValueField(field));
          schema.addField(dimensionFieldSpec);
          continue;
        case METRIC:
          final FieldSpec metricFieldSpec = new MetricFieldSpec(columnName, getColumnType(field));
          schema.addField(metricFieldSpec);
          continue;
        default:
          throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
      }
    }

    dataStream.close();
    return schema;
  }

  public static Schema extractSchemaFromAvroWithoutTime(File avroFile) throws IOException {
    DataFileStream<GenericRecord> dataStream =
        new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
    Schema schema = new Schema();

    for (final Field field : dataStream.getSchema().getFields()) {
      try {
        getColumnType(field);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while converting Avro field {} of type {}, field will not be in schema.",
            field.name(), field.schema().getType());
        continue;
      }
      final String columnName = field.name();
      final String pinotType = field.getProp("pinotType");

      final FieldSpec fieldSpec;
      if (pinotType != null && "METRIC".equals(pinotType)) {
        fieldSpec = new MetricFieldSpec();
      } else {
        fieldSpec = new DimensionFieldSpec();
      }

      fieldSpec.setName(columnName);
      fieldSpec.setDataType(getColumnType(dataStream.getSchema().getField(columnName)));
      fieldSpec.setSingleValueField(isSingleValueField(dataStream.getSchema().getField(columnName)));
      schema.addField(fieldSpec);
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
