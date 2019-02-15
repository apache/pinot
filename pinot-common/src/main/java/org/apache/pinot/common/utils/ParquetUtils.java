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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


public class ParquetUtils {
  /**
   * Get the parquet record iterator for the given file
   */
  public static Iterator<GenericRecord> getParquetReader(String fileName)
      throws IOException {
    Path dataFsPath = new Path(fileName);
    Configuration conf = new Configuration();
    ParquetReader<GenericRecord> parquet =
        AvroParquetReader.<GenericRecord>builder(dataFsPath).disableCompatibility().withDataModel(GenericData.get())
            .withConf(conf).build();

    return new Iterator<GenericRecord>() {
      private boolean hasNext = false;
      private GenericRecord next = advance();

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public GenericRecord next() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }

        GenericRecord toReturn = next;
        next = advance();
        return toReturn;
      }

      private GenericRecord advance() {
        try {
          GenericRecord next = parquet.read();
          hasNext = (next != null);

          if (hasNext == false) {
            parquet.close();
          }

          return next;
        } catch (IOException e) {
          throw new RuntimeException("Failed while reading parquet file: " + fileName, e);
        }
      }
    };
  }

  /**
   * Get parquet schema for the given file
   */
  public static Schema getParquetSchema(String fileName)
      throws IOException {
    Path dataFsPath = new Path(fileName);
    FileSystem fs = dataFsPath.getFileSystem(new Configuration());
    ParquetMetadata footer = ParquetFileReader.readFooter(fs.getConf(), dataFsPath);

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
        AvroParquetWriter.<GenericRecord>builder(new Path(fileName)).withSchema(schema).build();

    try {
      for (GenericRecord r : records) {
        writer.write(r);
      }
    } finally {
      writer.close();
    }
  }
}
