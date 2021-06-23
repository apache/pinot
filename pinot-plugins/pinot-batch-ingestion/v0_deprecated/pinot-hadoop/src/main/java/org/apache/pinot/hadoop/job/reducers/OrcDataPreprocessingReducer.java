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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OrcDataPreprocessingReducer extends Reducer<WritableComparable, OrcValue, NullWritable, OrcStruct> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrcDataPreprocessingReducer.class);

  private int _maxNumRecordsPerFile;
  private MultipleOutputs<NullWritable, OrcStruct> _multipleOutputs;
  private long _numRecords;
  private String _filePrefix;

  @Override
  public void setup(Context context) {
    Configuration configuration = context.getConfiguration();
    // If it's 0, the output file won't be split into multiple files.
    // If not, output file will be split when the number of records reaches this number.
    _maxNumRecordsPerFile = configuration.getInt(InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE, 0);
    if (_maxNumRecordsPerFile > 0) {
      LOGGER.info("Using multiple outputs strategy.");
      _multipleOutputs = new MultipleOutputs<>(context);
      _numRecords = 0L;
      _filePrefix = RandomStringUtils.randomAlphanumeric(4);
      LOGGER.info("Initialized OrcDataPreprocessingReducer with maxNumRecordsPerFile: {}", _maxNumRecordsPerFile);
    } else {
      LOGGER.info("Initialized OrcDataPreprocessingReducer without limit on maxNumRecordsPerFile");
    }
  }

  @Override
  public void reduce(WritableComparable key, Iterable<OrcValue> values, Context context)
      throws IOException, InterruptedException {
    if (_maxNumRecordsPerFile > 0) {
      for (final OrcValue value : values) {
        String fileName = _filePrefix + (_numRecords++ / _maxNumRecordsPerFile);
        _multipleOutputs.write(NullWritable.get(), (OrcStruct) value.value, fileName);
      }
    } else {
      for (final OrcValue value : values) {
        context.write(NullWritable.get(), (OrcStruct) value.value);
      }
    }
  }

  @Override
  public void cleanup(Context context)
      throws IOException, InterruptedException {
    LOGGER.info("Clean up reducer.");
    if (_multipleOutputs != null) {
      _multipleOutputs.close();
      _multipleOutputs = null;
    }
    LOGGER.info("Finished cleaning up reducer.");
  }
}
