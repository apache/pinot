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
package org.apache.pinot.hadoop.job.partitioners;

import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroDataPreprocessingPartitioner extends Partitioner<WritableComparable, AvroValue<GenericRecord>>
    implements Configurable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroDataPreprocessingPartitioner.class);

  private Configuration _conf;
  private String _partitionColumn;
  private PartitionFunction _partitionFunction;
  private String _partitionColumnDefaultNullValue;
  private AvroRecordExtractor _avroRecordExtractor;
  private int _numReducers = -1;

  private final AtomicInteger _counter = new AtomicInteger(0);

  @Override
  public void setConf(Configuration conf) {
    _conf = conf;
    _avroRecordExtractor = new AvroRecordExtractor();
    _partitionColumn = conf.get(InternalConfigConstants.PARTITION_COLUMN_CONFIG);
    String partitionFunctionName = conf.get(InternalConfigConstants.PARTITION_FUNCTION_CONFIG);
    String numPartitionsString = conf.get(InternalConfigConstants.NUM_PARTITIONS_CONFIG);
    int numPartitions = -1;
    if (_partitionColumn != null) {
      numPartitions = Integer.parseInt(numPartitionsString);
      _partitionFunction = PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions);
    } else {
      _numReducers = Integer.parseInt(conf.get(MRJobConfig.NUM_REDUCES));
    }
    _partitionColumnDefaultNullValue = conf.get(InternalConfigConstants.PARTITION_COLUMN_DEFAULT_NULL_VALUE);
    LOGGER.info(
        "Initialized AvroDataPreprocessingPartitioner with partitionColumn: {}, partitionFunction: {}, numPartitions:"
            + " {}, default null value: {}",
        _partitionColumn, partitionFunctionName, numPartitions, _partitionColumnDefaultNullValue);
  }

  @Override
  public Configuration getConf() {
    return _conf;
  }

  @Override
  public int getPartition(WritableComparable key, AvroValue<GenericRecord> value, int numPartitions) {
    if (_partitionColumn == null) {
      return Math.abs(_counter.getAndIncrement()) % _numReducers;
    } else {
      GenericRecord record = value.datum();
      Object object = record.get(_partitionColumn);
      String convertedValueString;
      if (object == null) {
        convertedValueString = _partitionColumnDefaultNullValue;
      } else {
        Object convertedValue = _avroRecordExtractor.convert(object);
        Preconditions
            .checkState(convertedValue != null, "Invalid value: %s for partition column: %s in record: %s", object,
                _partitionColumn, record);
        Preconditions.checkState(convertedValue instanceof Number || convertedValue instanceof String,
            "Value for partition column: %s must be either a Number or a String, found: %s in record: %s",
            _partitionColumn, convertedValue.getClass(), record);
        convertedValueString = convertedValue.toString();
      }
      // NOTE: Always partition with String type value because Broker uses String type value to prune segments
      return _partitionFunction.getPartition(convertedValueString);
    }
  }
}
