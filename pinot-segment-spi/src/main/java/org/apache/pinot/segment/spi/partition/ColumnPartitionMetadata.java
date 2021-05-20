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
package org.apache.pinot.segment.spi.partition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;


/**
 * Class for partition related column metadata:
 * <ul>
 *   <li>The name of the Partition function used to map the column values to their partitions</li>
 *   <li>Total number of partitions</li>
 *   <li>Set of partitions the column contains</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(using = ColumnPartitionMetadata.ColumnPartitionMetadataDeserializer.class)
public class ColumnPartitionMetadata {
  private final String _functionName;
  private final int _numPartitions;
  private final Set<Integer> _partitions;

  /**
   * Constructor for the class.
   *
   * @param functionName Name of the partition function
   * @param numPartitions Number of total partitions for this column
   * @param partitions Set of partitions the column contains
   */
  public ColumnPartitionMetadata(String functionName, int numPartitions, Set<Integer> partitions) {
    _functionName = functionName;
    _numPartitions = numPartitions;
    _partitions = partitions;
  }

  public String getFunctionName() {
    return _functionName;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public Set<Integer> getPartitions() {
    return _partitions;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ColumnPartitionMetadata) {
      ColumnPartitionMetadata that = (ColumnPartitionMetadata) obj;
      return _functionName.equals(that._functionName) && _numPartitions == that._numPartitions && _partitions
          .equals(that._partitions);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 37 * 37 * _functionName.hashCode() + 37 * _numPartitions + _partitions.hashCode();
  }

  /**
   * Helper method to extract partitions from configuration.
   * <p>
   * There are two format of partition strings:
   * <ul>
   *   <li>Integer format: e.g. {@code "0"}</li>
   *   <li>Range format (legacy): e.g. {@code "[0 5]"}</li>
   *   TODO: remove range format once all segments use integer format
   * </ul>
   */
  public static Set<Integer> extractPartitions(List partitionList) {
    Set<Integer> partitions = new HashSet<>();
    for (Object o : partitionList) {
      String partitionString = o.toString();
      if (partitionString.charAt(0) == '[') {
        // Range format
        addRangeToPartitions(partitionString, partitions);
      } else {
        partitions.add(Integer.parseInt(partitionString));
      }
    }
    return partitions;
  }

  /**
   * Helper method to add a partition range to a set of partitions.
   */
  private static void addRangeToPartitions(String rangeString, Set<Integer> partitions) {
    int delimiterIndex = rangeString.indexOf(' ');
    int start = Integer.parseInt(rangeString.substring(1, delimiterIndex));
    int end = Integer.parseInt(rangeString.substring(delimiterIndex + 1, rangeString.length() - 1));
    for (int i = start; i <= end; i++) {
      partitions.add(i);
    }
  }

  /**
   * Custom deserializer for {@link ColumnPartitionMetadata}.
   * <p>
   * This deserializer understands the legacy range format: {@code "partitionRanges":"[0 0],[1 1]"}
   * TODO: remove custom deserializer once all segments use integer format
   */
  public static class ColumnPartitionMetadataDeserializer extends JsonDeserializer<ColumnPartitionMetadata> {
    private static final String FUNCTION_NAME_KEY = "functionName";
    private static final String NUM_PARTITIONS_KEY = "numPartitions";
    private static final String PARTITIONS_KEY = "partitions";

    // DO NOT CHANGE: for backward-compatibility
    private static final String LEGACY_PARTITIONS_KEY = "partitionRanges";
    private static final char LEGACY_PARTITION_DELIMITER = ',';

    @Override
    public ColumnPartitionMetadata deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode jsonMetadata = p.getCodec().readTree(p);
      Set<Integer> partitions = new HashSet<>();
      JsonNode jsonPartitions = jsonMetadata.get(PARTITIONS_KEY);
      if (jsonPartitions != null) {
        // Integer format: "partitions":[0,1,5]
        for (JsonNode jsonPartition : jsonPartitions) {
          partitions.add(jsonPartition.asInt());
        }
      } else {
        // Legacy format: "partitionRanges":"[0 1],[5 5]"
        String partitionRanges = jsonMetadata.get(LEGACY_PARTITIONS_KEY).asText();
        for (String partitionRange : StringUtils.split(partitionRanges, LEGACY_PARTITION_DELIMITER)) {
          addRangeToPartitions(partitionRange, partitions);
        }
      }
      return new ColumnPartitionMetadata(jsonMetadata.get(FUNCTION_NAME_KEY).asText(),
          jsonMetadata.get(NUM_PARTITIONS_KEY).asInt(), partitions);
    }
  }
}
