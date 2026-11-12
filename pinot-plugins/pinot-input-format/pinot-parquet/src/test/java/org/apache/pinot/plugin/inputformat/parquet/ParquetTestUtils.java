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
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;


public class ParquetTestUtils {

  private ParquetTestUtils() {
  }

  /// Encodes an instant as little-endian INT96 with a nonnegative nanosecond-of-day and Julian day number.
  static byte[] int96Bytes(Instant instant) {
    long secondsPerDay = TimeUnit.DAYS.toSeconds(1);
    long day = Math.floorDiv(instant.getEpochSecond(), secondsPerDay);
    long nanosOfDay = Math.floorMod(instant.getEpochSecond(), secondsPerDay) * TimeUnit.SECONDS.toNanos(1)
        + instant.getNano();
    return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay)
        .putInt(Math.toIntExact(day + ParquetUtils.JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH)).array();
  }

  /// Returns a ParquetWriter with the given path and schema.
  public static ParquetWriter<GenericRecord> getParquetAvroWriter(Path path, Schema schema)
      throws IOException {
    OutputFile outputFile = HadoopOutputFile.fromPath(path, ParquetUtils.getParquetHadoopConfiguration());
    return AvroParquetWriter.<GenericRecord>builder(outputFile).withSchema(schema)
        .withConf(ParquetUtils.getParquetHadoopConfiguration())
        .build();
  }
}
