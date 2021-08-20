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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.hadoop.job.InternalConfigConstants.NUM_PARTITIONS_CONFIG;
import static org.apache.pinot.hadoop.job.InternalConfigConstants.PARTITION_COLUMN_CONFIG;
import static org.apache.pinot.hadoop.job.InternalConfigConstants.PARTITION_FUNCTION_CONFIG;


public class GenericPartitioner<T> extends Partitioner<T, AvroValue<GenericRecord>> implements Configurable {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericPartitioner.class);
  private Configuration _configuration;
  private String _partitionColumn;
  private int _numPartitions;
  private PartitionFunction _partitionFunction;

  @Override
  public void setConf(Configuration conf) {
    _configuration = conf;
    _partitionColumn = _configuration.get(PARTITION_COLUMN_CONFIG);
    _numPartitions = Integer.parseInt(_configuration.get(NUM_PARTITIONS_CONFIG));
    _partitionFunction = PartitionFunctionFactory.getPartitionFunction(_configuration.get(PARTITION_FUNCTION_CONFIG, null), _numPartitions);

    LOGGER.info("The partition function is: " + _partitionFunction.getClass().getName());
    LOGGER.info("The partition column is: " + _partitionColumn);
    LOGGER.info("Total number of partitions is: " + _numPartitions);
  }

  @Override
  public Configuration getConf() {
    return _configuration;
  }

  @Override
  public int getPartition(T genericRecordAvroKey, AvroValue<GenericRecord> genericRecordAvroValue, int numPartitions) {
    final GenericRecord inputRecord = genericRecordAvroValue.datum();
    final Object partitionColumnValue = inputRecord.get(_partitionColumn);
    return _partitionFunction.getPartition(partitionColumnValue);
  }
}
