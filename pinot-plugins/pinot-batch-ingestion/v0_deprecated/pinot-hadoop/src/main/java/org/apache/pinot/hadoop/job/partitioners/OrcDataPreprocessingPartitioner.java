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
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.apache.pinot.hadoop.utils.preprocess.OrcUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OrcDataPreprocessingPartitioner extends Partitioner<WritableComparable, OrcValue> implements Configurable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrcDataPreprocessingPartitioner.class);

  private Configuration _conf;
  private String _partitionColumn;
  private PartitionFunction _partitionFunction;
  private int _partitionColumnId = -1;

  @Override
  public void setConf(Configuration conf) {
    _conf = conf;
    _partitionColumn = conf.get(InternalConfigConstants.PARTITION_COLUMN_CONFIG);
    String partitionFunctionName = conf.get(InternalConfigConstants.PARTITION_FUNCTION_CONFIG);
    int numPartitions = Integer.parseInt(conf.get(InternalConfigConstants.NUM_PARTITIONS_CONFIG));
    _partitionFunction = PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions);
    LOGGER.info(
        "Initialized OrcDataPreprocessingPartitioner with partitionColumn: {}, partitionFunction: {}, numPartitions: "
            + "{}",
        _partitionColumn,
        partitionFunctionName, numPartitions);
  }

  @Override
  public Configuration getConf() {
    return _conf;
  }

  @Override
  public int getPartition(WritableComparable key, OrcValue value, int numPartitions) {
    OrcStruct orcStruct = (OrcStruct) value.value;
    if (_partitionColumnId == -1) {
      List<String> fieldNames = orcStruct.getSchema().getFieldNames();
      _partitionColumnId = fieldNames.indexOf(_partitionColumn);
      Preconditions.checkState(_partitionColumnId != -1, "Failed to find partition column: %s in the ORC fields: %s",
          _partitionColumn, fieldNames);
      LOGGER.info("Field id for partition column: {} is: {}", _partitionColumn, _partitionColumnId);
    }
    WritableComparable partitionColumnValue = orcStruct.getFieldValue(_partitionColumnId);
    Object convertedValue;
    try {
      convertedValue = OrcUtils.convert(partitionColumnValue);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Caught exception while processing partition column: %s, id: %d in ORC struct: %s",
              _partitionColumn, _partitionColumnId, orcStruct),
          e);
    }
    // NOTE: Always partition with String type value because Broker uses String type value to prune segments
    return _partitionFunction.getPartition(convertedValue.toString());
  }
}
