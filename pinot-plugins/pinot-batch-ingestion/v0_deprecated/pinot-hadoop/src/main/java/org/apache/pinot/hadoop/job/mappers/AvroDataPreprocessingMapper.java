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
package org.apache.pinot.hadoop.job.mappers;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.apache.pinot.hadoop.utils.preprocess.DataPreprocessingUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroDataPreprocessingMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, WritableComparable, AvroValue<GenericRecord>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroDataPreprocessingMapper.class);

  private String _sortingColumn = null;
  private FieldSpec.DataType _sortingColumnType = null;
  private AvroRecordExtractor _avroRecordExtractor;

  @Override
  public void setup(Context context) {
    Configuration configuration = context.getConfiguration();
    _avroRecordExtractor = new AvroRecordExtractor();
    String sortingColumnConfig = configuration.get(InternalConfigConstants.SORTING_COLUMN_CONFIG);
    if (sortingColumnConfig != null) {
      _sortingColumn = sortingColumnConfig;
      _sortingColumnType = FieldSpec.DataType.valueOf(configuration.get(InternalConfigConstants.SORTING_COLUMN_TYPE));
      LOGGER.info("Initialized AvroDataPreprocessingMapper with sortingColumn: {} of type: {}", _sortingColumn, _sortingColumnType);
    } else {
      LOGGER.info("Initialized AvroDataPreprocessingMapper without sorting column");
    }
  }

  @Override
  public void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
      throws IOException, InterruptedException {
    GenericRecord record = key.datum();
    if (_sortingColumn != null) {
      Object object = record.get(_sortingColumn);
      Preconditions.checkState(object != null, "Failed to find value for sorting column: %s in record: %s", _sortingColumn, record);
      Object convertedValue = _avroRecordExtractor.convert(object);
      Preconditions.checkState(convertedValue != null, "Invalid value: %s for sorting column: %s in record: %s", object, _sortingColumn, record);
      WritableComparable outputKey;
      try {
        outputKey = DataPreprocessingUtils.convertToWritableComparable(convertedValue, _sortingColumnType);
      } catch (Exception e) {
        throw new IllegalStateException(String.format("Caught exception while processing sorting column: %s in record: %s", _sortingColumn, record), e);
      }
      context.write(outputKey, new AvroValue<>(record));
    } else {
      context.write(NullWritable.get(), new AvroValue<>(record));
    }
  }
}
