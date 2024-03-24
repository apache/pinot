/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.creator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.apache.pinot.plugin.inputformat.avro.AvroSchemaUtil;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentTestUtils {
  private SegmentTestUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentTestUtils.class);

  @Nonnull
  public static SegmentGeneratorConfig getSegmentGeneratorConfigWithoutTimeColumn(@Nonnull File avroFile,
      @Nonnull File outputDir, @Nonnull String tableName)
      throws IOException {
    SegmentGeneratorConfig segmentGeneratorConfig =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build(),
            extractSchemaFromAvroWithoutTime(avroFile));
    segmentGeneratorConfig.setInputFilePath(avroFile.getAbsolutePath());
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
    segmentGeneratorConfig.setTableName(tableName);
    return segmentGeneratorConfig;
  }

  public static SegmentGeneratorConfig getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro, File outputDir,
      String timeColumn, TimeUnit timeUnit, String tableName)
      throws IOException {
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(false);
    ingestionConfig.setSegmentTimeValueCheck(false);
    final SegmentGeneratorConfig segmentGenSpec =
        new SegmentGeneratorConfig(
            new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setIngestionConfig(ingestionConfig)
                .build(),
            extractSchemaFromAvroWithoutTime(inputAvro));
    segmentGenSpec.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenSpec.setTimeColumnName(timeColumn);
    segmentGenSpec.setSegmentTimeUnit(timeUnit);
    segmentGenSpec.setFormat(FileFormat.AVRO);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setTableName(tableName);
    segmentGenSpec.setOutDir(outputDir.getAbsolutePath());
    segmentGenSpec.getSchema().getAllFieldSpecs()
        .forEach(fs -> segmentGenSpec.setIndexOn(StandardIndexes.inverted(), IndexConfig.ENABLED, fs.getName()));
    return segmentGenSpec;
  }

  public static SegmentGeneratorConfig getSegmentGeneratorConfigWithSchema(File inputAvro, File outputDir,
      String tableName, TableConfig tableConfig, Schema schema) {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
    segmentGeneratorConfig.setFormat(FileFormat.AVRO);
    segmentGeneratorConfig.setSegmentVersion(SegmentVersion.v1);
    segmentGeneratorConfig.setTableName(tableName);
    return segmentGeneratorConfig;
  }

  public static SegmentGeneratorConfig getSegmentGeneratorConfig(File inputFile, FileFormat inputFormat, File outputDir,
      String tableName, TableConfig tableConfig, Schema schema) {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(inputFile.getAbsolutePath());
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
    segmentGeneratorConfig.setFormat(inputFormat);
    segmentGeneratorConfig.setTableName(tableName);
    return segmentGeneratorConfig;
  }

  public static Schema extractSchemaFromAvroWithoutTime(File avroFile)
      throws IOException {
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
      return AvroSchemaUtil.valueOf(elementSchema.getType());
    } else {
      return AvroSchemaUtil.valueOf(type);
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
