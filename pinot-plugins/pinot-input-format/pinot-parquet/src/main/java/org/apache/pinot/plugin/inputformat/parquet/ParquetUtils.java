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
package org.apache.pinot.plugin.inputformat.parquet;

import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;


public class ParquetUtils {
  private static final String DEFAULT_FS = "file:///";

  private ParquetUtils() {
  }

  /**
   * Returns a ParquetReader with the given path.
   */
  public static ParquetReader<GenericRecord> getParquetAvroReader(Path path)
      throws IOException {
    //noinspection unchecked
    return AvroParquetReader.<GenericRecord>builder(path).disableCompatibility().withDataModel(GenericData.get())
        .withConf(getParquetAvroReaderConfiguration()).build();
  }

  /**
   * Returns a ParquetWriter with the given path and schema.
   */
  public static ParquetWriter<GenericRecord> getParquetAvroWriter(Path path, Schema schema)
      throws IOException {
    return AvroParquetWriter.<GenericRecord>builder(path).withSchema(schema)
        .withConf(getParquetAvroReaderConfiguration()).build();
  }

  /**
   * Returns the schema for the given Parquet file path.
   */
  public static Schema getParquetAvroSchema(Path path)
      throws IOException {
    ParquetMetadata footer =
        ParquetFileReader.readFooter(getParquetAvroReaderConfiguration(), path, ParquetMetadataConverter.NO_FILTER);
    Map<String, String> metaData = footer.getFileMetaData().getKeyValueMetaData();
    String schemaString = metaData.get("parquet.avro.schema");
    if (schemaString == null) {
      // Try the older property
      schemaString = metaData.get("avro.schema");
    }
    if (schemaString != null) {
      return new Schema.Parser().parse(schemaString);
    } else {
      MessageType parquetSchema = footer.getFileMetaData().getSchema();
      return new AvroSchemaConverter().convert(parquetSchema);
    }
  }

  private static Configuration getParquetAvroReaderConfiguration() {
    // The file path used in ParquetRecordReader is a local file path without prefix 'file:///',
    // so we have to make sure that the configuration item 'fs.defaultFS' is set to 'file:///'
    // in case that user's hadoop conf overwrite this item
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", DEFAULT_FS);
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return conf;
  }
}
