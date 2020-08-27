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
package org.apache.pinot.core.segment.processing.framework;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.core.segment.processing.collector.Collector;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reducer phase of the SegmentProcessorFramework
 * Reads the avro files in the input directory and creates output avro files in the reducer output directory.
 * The avro files in the input directory are expected to contain data for only 1 partition
 * Performs operations on that partition data as follows:
 * - concatenation/rollup of records
 * - split
 * - TODO: dedup
 */
public class SegmentReducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentReducer.class);
  private static final int MAX_RECORDS_TO_COLLECT = 5_000_000;

  private final File _reducerInputDir;
  private final File _reducerOutputDir;

  private final String _reducerId;
  private final Schema _pinotSchema;
  private final org.apache.avro.Schema _avroSchema;
  private final Collector _collector;
  private final int _numRecordsPerPart;

  public SegmentReducer(File reducerInputDir, SegmentReducerConfig reducerConfig, File reducerOutputDir) {
    _reducerInputDir = reducerInputDir;
    _reducerOutputDir = reducerOutputDir;

    _reducerId = reducerConfig.getReducerId();
    _pinotSchema = reducerConfig.getPinotSchema();
    _avroSchema = SegmentProcessorUtils.convertPinotSchemaToAvroSchema(_pinotSchema);
    _collector = CollectorFactory.getCollector(reducerConfig.getCollectorConfig(), _pinotSchema);
    _numRecordsPerPart = reducerConfig.getNumRecordsPerPart();
    LOGGER.info("Initialized reducer with id: {}, input dir: {}, output dir: {}, collector: {}, numRecordsPerPart: {}",
        _reducerId, _reducerInputDir, _reducerOutputDir, _collector.getClass(), _numRecordsPerPart);
  }

  /**
   * Reads the avro files in the input directory.
   * Performs configured operations and outputs to other avro file(s) in the reducer output directory.
   */
  public void reduce()
      throws Exception {

    int part = 0;
    for (File inputFile : _reducerInputDir.listFiles()) {

      // FIXME: create via plugins before submitting PR
      RecordReader avroRecordReader = new AvroRecordReader();
      avroRecordReader.init(inputFile, _pinotSchema.getColumnNames(), null);
//      RecordReader avroRecordReader = RecordReaderFactory
//          .getRecordReaderByClass("org.apache.pinot.plugin.inputformat.avro.AvroRecordReader", inputFile,
//              _pinotSchema.getColumnNames(), null);

      while (avroRecordReader.hasNext()) {
        GenericRow next = avroRecordReader.next();

        // Aggregations
        _collector.collect(next);

        // Exceeded max records allowed to collect. Flush
        if (_collector.size() == MAX_RECORDS_TO_COLLECT) {
          int numFiles = flushRecords(_collector, part);
          part += numFiles;
          _collector.reset();
        }
      }
    }
    if (_collector.size() > 0) {
      flushRecords(_collector, part);
      _collector.reset();
    }
  }

  /**
   * Flushes all records from the collector into a part files in the reducer output directory
   */
  private int flushRecords(Collector collector, int partNumber)
      throws IOException {
    GenericData.Record reusableRecord = new GenericData.Record(_avroSchema);
    Iterator<GenericRow> collectionIt = collector.iterator();

    DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
    recordWriter.create(_avroSchema, new File(_reducerOutputDir, createReducerOutputFileName(_reducerId, partNumber++)));

    int numRecords = 0;
    while (collectionIt.hasNext()) {
      SegmentProcessorUtils.convertGenericRowToAvroRecord(collectionIt.next(), reusableRecord);
      recordWriter.append(reusableRecord);
      numRecords++;
      if (numRecords == _numRecordsPerPart) {
        recordWriter.close();
        numRecords = 0;
        if (collectionIt.hasNext()) {
          recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
          recordWriter.create(_avroSchema, new File(_reducerOutputDir, createReducerOutputFileName(_reducerId, partNumber++)));
        }
      }
    }
    if (numRecords > 0) {
      recordWriter.close();
    }
    return partNumber;
  }

  public static String createReducerOutputFileName(String reducerId, int part) {
    return "reducer_" + reducerId + "_" + part + ".avro";
  }

  /**
   * Cleans up reducer state
   */
  public void cleanup() {
  }
}
