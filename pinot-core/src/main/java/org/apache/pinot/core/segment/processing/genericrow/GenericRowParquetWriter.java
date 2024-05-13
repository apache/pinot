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
package org.apache.pinot.core.segment.processing.genericrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.plugin.inputformat.parquet.ParquetUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * File writer for {@link GenericRow}. The writer will generate to 2 files, one for the offsets (BIG_ENDIAN) and one for
 * the actual data (NATIVE_ORDER). The generated files can be read by the {@link GenericRowFileReader}. There is no
 * version control for the files generated because the files should only be used as intermediate format and read in the
 * same host (different host might have different NATIVE_ORDER).
 *
 * TODO: Consider using ByteBuffer instead of OutputStream.
 */
public class GenericRowParquetWriter implements FileWriter<GenericRow> {
  private final Schema _pinotSchema;
  private final org.apache.avro.Schema _avroSchema;
  private File _dataFile;

  public GenericRowParquetWriter(File dataFile, Schema pinotSchema)
      throws FileNotFoundException {
    _pinotSchema = pinotSchema;
    _dataFile = dataFile;
    _avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(pinotSchema);
  }

  private GenericRecord convertGenericRowToGenericRecord(GenericRow genericRow) {
    GenericRecord record = new GenericData.Record(_avroSchema);
    for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
      record.put(fieldSpec.getName(), genericRow.getValue(fieldSpec.getName()));
    }
    return record;
  }

  /**
   * Writes the given row into the files.
   */
  public void write(GenericRow genericRow)
      throws IOException {
    GenericRecord record = convertGenericRowToGenericRecord(genericRow);
    try (ParquetWriter<GenericRecord> writer = ParquetUtils.getParquetAvroWriter(
        new Path(_dataFile.getAbsolutePath()), _avroSchema)) {
      writer.write(record);
    }
  }

  public long writeData(GenericRow genericRow)
      throws IOException {
    GenericRecord record = convertGenericRowToGenericRecord(genericRow);
    ParquetWriter<GenericRecord> writer =
        ParquetUtils.getParquetAvroWriter(new Path(_dataFile.getAbsolutePath()), _avroSchema);
    writer.write(record);
    return writer.getDataSize();
  }

  @Override
  public void close()
      throws IOException {
  }
}
