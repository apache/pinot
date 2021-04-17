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
package org.apache.pinot.hadoop.job.reducers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPreprocessingReducer<T>
    extends Reducer<T, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreprocessingReducer.class);

  private AvroMultipleOutputs _multipleOutputs;
  private AtomicInteger _counter;
  private int _maxNumberOfRecords;
  private String _filePrefix;

  @Override
  public void setup(Context context) {
    LOGGER.info("Using multiple outputs strategy.");
    Configuration configuration = context.getConfiguration();
    _multipleOutputs = new AvroMultipleOutputs(context);
    _counter = new AtomicInteger();
    // If it's 0, the output file won't be split into multiple files.
    // If not, output file will be split when the number of records reaches this number.
    _maxNumberOfRecords = configuration.getInt(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE, 0);
    LOGGER.info("Maximum number of records per file: {}", _maxNumberOfRecords);
    _filePrefix = RandomStringUtils.randomAlphanumeric(4);
  }

  @Override
  public void reduce(final T inputRecord, final Iterable<AvroValue<GenericRecord>> values, final Context context)
      throws IOException, InterruptedException {
    for (final AvroValue<GenericRecord> value : values) {
      String fileName = generateFileName();
      _multipleOutputs.write(new AvroKey<>(value.datum()), NullWritable.get(), fileName);
    }
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    LOGGER.info("Clean up reducer.");
    if (_multipleOutputs != null) {
      _multipleOutputs.close();
      _multipleOutputs = null;
    }
    LOGGER.info("Finished cleaning up reducer.");
  }

  private String generateFileName() {
    if (_maxNumberOfRecords == 0) {
      return _filePrefix;
    } else {
      return _filePrefix + (_counter.getAndIncrement() / _maxNumberOfRecords);
    }
  }
}
