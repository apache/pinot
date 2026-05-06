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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.pinot.spi.utils.TimestampUtils;


public class ParquetUtils {
  private ParquetUtils() {
  }

  private static final String DEFAULT_FS = "file:///";
  private static final String AVRO_SCHEMA_METADATA_KEY = "parquet.avro.schema";
  private static final String OLD_AVRO_SCHEMA_METADATA_KEY = "avro.schema";

  /// Number of days from the Julian day epoch (4713-01-01 BC) to the Unix day epoch (1970-01-01).
  public static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;

  private static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);

  /**
   * Returns a ParquetReader with the given path.
   */
  public static ParquetReader<GenericRecord> getParquetAvroReader(Path path)
      throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(path, getParquetHadoopConfiguration());
    return AvroParquetReader.<GenericRecord>builder(inputFile).disableCompatibility().withDataModel(GenericData.get())
        .build();
  }

  /**
   * Returns the schema for the given Parquet file path.
   */
  public static Schema getParquetAvroSchema(Path path)
      throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(path, getParquetHadoopConfiguration());
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      Map<String, String> metaData = reader.getFileMetaData().getKeyValueMetaData();
      if (hasAvroSchemaInFileMetadata(path)) {
        String schemaString = metaData.get(AVRO_SCHEMA_METADATA_KEY);
        if (schemaString == null) {
          // Try the older property
          schemaString = metaData.get(OLD_AVRO_SCHEMA_METADATA_KEY);
        }
        return new Schema.Parser().parse(schemaString);
      } else {
        MessageType parquetSchema = reader.getFileMetaData().getSchema();
        return new AvroSchemaConverter().convert(parquetSchema);
      }
    }
  }

  public static boolean hasAvroSchemaInFileMetadata(Path path)
      throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(path, getParquetHadoopConfiguration());
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      Map<String, String> metaData = reader.getFileMetaData().getKeyValueMetaData();
      return metaData.containsKey(AVRO_SCHEMA_METADATA_KEY) || metaData.containsKey(OLD_AVRO_SCHEMA_METADATA_KEY);
    }
  }

  public static Configuration getParquetHadoopConfiguration() {
    // The file path used in ParquetRecordReader is a local file path without prefix 'file:///',
    // so we have to make sure that the configuration item 'fs.defaultFS' is set to 'file:///'
    // in case that user's hadoop conf overwrite this item
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", DEFAULT_FS);
    // To read Int96 as bytes.
    conf.set(AvroReadSupport.READ_INT96_AS_FIXED, "true");
    // Tell parquet-avro NOT to surface the 3-level Parquet LIST encoding's repeated wrapper as an Avro record.
    // With the default (true), a Parquet `LIST<STRING>` reads back as Avro `array<record<element: string>>` and
    // we have to strip that wrapper ourselves; with this off, parquet-avro materializes the Avro schema flat
    // (`array<string>`), matching Apache Arrow's Parquet reader behavior. This eliminates the wrapper-stripping
    // ambiguity entirely — real user records (e.g. `array<record<UserTag, [element: string]>>` written from an
    // Avro source) survive untouched because their Avro schema in the file metadata is honored as-is.
    conf.set("parquet.avro.add-list-element-records", "false");
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return conf;
  }

  /// Converts an epoch `value` interpreted in `unit` (millis / micros / nanos) to [Timestamp].
  /// Sub-millisecond precision is preserved via [Timestamp#setNanos] (delegates to [TimestampUtils]).
  public static Timestamp convertLongToTimestamp(long value, LogicalTypeAnnotation.TimeUnit unit) {
    switch (unit) {
      case MILLIS:
        return new Timestamp(value);
      case MICROS:
        return TimestampUtils.fromMicrosSinceEpoch(value);
      case NANOS:
        return TimestampUtils.fromNanosSinceEpoch(value);
      default:
        throw new IllegalArgumentException("Unsupported timestamp unit: " + unit);
    }
  }

  /// Converts a 12-byte INT96 timestamp to `Long` epoch nanos. INT96 stores the timestamp split as bytes
  /// 0..7 (nanos within the day, long, little-endian) and bytes 8..11 (Julian day number, int, little-endian).
  /// Nanos is INT96's natural unit since this physical encoding carries nanosecond precision; pair with
  /// [TimestampUtils#fromNanosSinceEpoch] for a [Timestamp].
  public static long convertInt96ToEpochNanos(byte[] int96Bytes) {
    ByteBuffer buf = ByteBuffer.wrap(int96Bytes).order(ByteOrder.LITTLE_ENDIAN);
    long days = buf.getInt(8) - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH;
    long nanosOfDay = buf.getLong(0);
    return days * NANOS_PER_DAY + nanosOfDay;
  }
}
