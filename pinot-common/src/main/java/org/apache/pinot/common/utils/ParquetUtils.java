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
package org.apache.pinot.common.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.List;


public class ParquetUtils {
  /**
   * Get a ParquetReader with the given file.
   * @param fileName the parquet file to read
   * @return a ParquetReader
   * @throws IOException
   */
  public static ParquetReader<GenericRecord> getParquetReader(String fileName)
      throws IOException {
    Path dataFsPath = new Path(fileName);
    return AvroParquetReader.<GenericRecord>builder(dataFsPath).disableCompatibility().withDataModel(GenericData.get())
        .withConf(getConfiguration()).build();
  }

  /**
   * Read the parquet file schema
   * @param fileName
   * @return the parquet file schema
   * @throws IOException
   */
  public static Schema getParquetSchema(String fileName)
      throws IOException {
    Path dataFsPath = new Path(fileName);
    ParquetMetadata footer = ParquetFileReader.readFooter(getConfiguration(), dataFsPath);

    String schemaString = footer.getFileMetaData().getKeyValueMetaData().get("parquet.avro.schema");
    if (schemaString == null) {
      // try the older property
      schemaString = footer.getFileMetaData().getKeyValueMetaData().get("avro.schema");
    }

    if (schemaString != null) {
      return new Schema.Parser().parse(schemaString);
    } else {
      return new AvroSchemaConverter().convert(footer.getFileMetaData().getSchema());
    }
  }

  /**
   * write records as parquet file
   */
  public static void writeParquetRecord(String fileName, Schema schema, List<GenericRecord> records)
      throws IOException {
    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new Path(fileName)).withSchema(schema).withConf(getConfiguration())
            .build();

    try {
      for (GenericRecord r : records) {
        writer.write(r);
      }
    } finally {
      writer.close();
    }
  }

  private static Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return conf;
  }
}
