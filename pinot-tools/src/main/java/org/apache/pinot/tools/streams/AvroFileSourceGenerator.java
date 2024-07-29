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
package org.apache.pinot.tools.streams;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.Strings;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generates Pinot Real Time Source by an AvroFile.
 * It will keep looping the same file and produce data output. We can pass in a lambda function to compute
 * time index based on row number.
 */
public class AvroFileSourceGenerator implements PinotSourceDataGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSourceGenerator.class);
  private DataFileStream<GenericRecord> _avroDataStream;
  private final Schema _pinotSchema;
  private long _rowsProduced;
  // If this var is null, we will not set time index column
  private final String _timeColumnName;
  private final Function<Long, Long> _rowNumberToTimeIndex;
  private final File _avroFile;
  private final int _rowsPerBatch;

  /**
   * Reads the avro file, produce the rows, and then keep looping without setting time index
   * @param pinotSchema the Pinot Schema so the avro rows can be produced
   * @param avroFile the avro file as source.
   */
  public AvroFileSourceGenerator(Schema pinotSchema, File avroFile) {
    this(pinotSchema, avroFile, 1, null, null);
  }

  /**
   * Reads the avro file, produce the rows, and keep looping, allows customization of time index by a lambda function
   * @param pinotSchema the Pinot Schema so the avro rows can be produced
   * @param avroFile the avro file as source.
   * @param rowsPerBatch in one batch, return several rows at the same time
   * @param timeColumnName the time column name for customizing/overriding time index. Null for skipping customization.
   * @param rowNumberToTimeIndex the lambda to compute time index based on row number. Null for skipping customization.
   */
  public AvroFileSourceGenerator(Schema pinotSchema, File avroFile, int rowsPerBatch,
      @Nullable String timeColumnName, @Nullable Function<Long, Long> rowNumberToTimeIndex) {
    _pinotSchema = pinotSchema;
    _rowsProduced = 0;
    _rowNumberToTimeIndex = rowNumberToTimeIndex;
    _timeColumnName = timeColumnName;
    if (!Strings.isNullOrEmpty(_timeColumnName)) {
      DateTimeFieldSpec timeColumnSpec = pinotSchema.getSpecForTimeColumn(timeColumnName);
      Preconditions.checkNotNull(timeColumnSpec,
          "Time column " + timeColumnName + " is not found in schema, or is not a valid DateTime column");
    }
    _avroFile = avroFile;
    _rowsPerBatch = rowsPerBatch;
  }

  @Override
  public void init(Properties properties) {
  }

  @Override
  public List<StreamDataProducer.RowWithKey> generateRows() {
    List<StreamDataProducer.RowWithKey> retVal = new ArrayList<>();
    ensureStream();
    int rowsInCurrentBatch = 0;
    while (_avroDataStream.hasNext() && rowsInCurrentBatch < _rowsPerBatch) {
      GenericRecord record = _avroDataStream.next();
      GenericRecord message = new GenericData.Record(AvroUtils.getAvroSchemaFromPinotSchema(_pinotSchema));
      for (FieldSpec spec : _pinotSchema.getDimensionFieldSpecs()) {
        message.put(spec.getName(), record.get(spec.getName()));
      }

      for (FieldSpec spec : _pinotSchema.getMetricFieldSpecs()) {
        message.put(spec.getName(), record.get(spec.getName()));
      }
      message.put(_timeColumnName, _rowNumberToTimeIndex.apply(_rowsProduced));
      retVal.add(new StreamDataProducer.RowWithKey(null, message.toString().getBytes(StandardCharsets.UTF_8)));
      _rowsProduced += 1;
      rowsInCurrentBatch += 1;
    }
    return retVal;
  }

  @Override
  public void close()
      throws Exception {
    _avroDataStream.close();
  }

  // Re-opens file stream if the file has reached its end.
  private void ensureStream() {
    try {
      if (_avroDataStream != null && !_avroDataStream.hasNext()) {
        _avroDataStream.close();
        _avroDataStream = null;
      }
      if (_avroDataStream == null) {
        _avroDataStream = new DataFileStream<>(new FileInputStream(_avroFile.getPath()), new GenericDatumReader<>());
      }
    } catch (IOException ex) {
      LOGGER.error("Failed to open/close {}", _avroFile.getPath(), ex);
      throw new RuntimeException("Failed to open/close " + _avroFile.getPath(), ex);
    }
  }
}
