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
package org.apache.pinot.segment.spi.partition.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction;
import org.apache.pinot.spi.utils.JsonUtils;


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
  private final String _functionExpr;
  private final int _numPartitions;
  private final Map<String, String> _functionConfig;
  private final Set<Integer> _partitions;

  /// Constructor for the class.
  ///
  /// @param functionName Name of the partition function
  /// @param numPartitions Number of total partitions for this column
  /// @param partitions Set of partitions the column contains
  /// @param functionConfig Configuration required by partition function.
  /// @deprecated Use [Set)][#ColumnPartitionMetadata(PartitionFunction,] instead, which derives all fields
  ///             directly from the [PartitionFunction] contract and keeps them consistent.
  ///             TODO: remove after release 1.7.0.
  @Deprecated
  public ColumnPartitionMetadata(String functionName, int numPartitions, Set<Integer> partitions,
      @Nullable Map<String, String> functionConfig) {
    _functionName = functionName;
    _functionExpr = null;
    _numPartitions = numPartitions;
    _partitions = partitions;
    _functionConfig = functionConfig;
  }

  private ColumnPartitionMetadata(@Nullable String functionName, int numPartitions, Set<Integer> partitions,
      @Nullable Map<String, String> functionConfig, @Nullable String functionExpr) {
    _functionName = functionName;
    _functionExpr = normalizeOptionalText(functionExpr);
    _numPartitions = numPartitions;
    _partitions = partitions;
    _functionConfig = functionConfig;
  }

  public ColumnPartitionMetadata(PartitionFunction partitionFunction, Set<Integer> partitions) {
    this(
        // Expression-mode uses "FunctionExpr" as a stable sentinel for the function name field.
        // This ensures old brokers (which call jsonMetadata.get("functionName").asText() without
        // a null guard) receive a non-null string rather than NPE-ing on a missing JSON key.
        // Old brokers fail gracefully with IllegalArgumentException ("No enum constant for: FunctionExpr")
        // and fall back to querying all segments rather than corrupting routing state.
        partitionFunction.getFunctionExpr() != null ? PartitionPipelineFunction.NAME
            : partitionFunction.getName(),
        partitionFunction.getNumPartitions(), partitions, partitionFunction.getFunctionConfig(),
        partitionFunction.getFunctionExpr());
  }

  @Nullable
  public String getFunctionName() {
    return _functionName;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  @Nullable
  public String getFunctionExpr() {
    return _functionExpr;
  }

  public Set<Integer> getPartitions() {
    return _partitions;
  }

  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ColumnPartitionMetadata) {
      ColumnPartitionMetadata that = (ColumnPartitionMetadata) obj;
      return Objects.equals(_functionName, that._functionName) && _numPartitions == that._numPartitions
          && _partitions.equals(that._partitions) && Objects.equals(_functionConfig, that._functionConfig)
          && Objects.equals(_functionExpr, that._functionExpr);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_functionName, _numPartitions, _partitions, _functionConfig, _functionExpr);
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
  public static IntSet extractPartitions(List<Object> partitionList) {
    IntSet partitions = new IntOpenHashSet(partitionList.size());
    for (Object partition : partitionList) {
      String partitionString = partition.toString();
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
  private static void addRangeToPartitions(String rangeString, IntSet partitions) {
    int delimiterIndex = rangeString.indexOf(' ');
    int start = Integer.parseInt(rangeString.substring(1, delimiterIndex));
    int end = Integer.parseInt(rangeString.substring(delimiterIndex + 1, rangeString.length() - 1));
    for (int i = start; i <= end; i++) {
      partitions.add(i);
    }
  }

  @Nullable
  private static String normalizeOptionalText(@Nullable String value) {
    return StringUtils.isBlank(value) ? null : value;
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
    private static final String FUNCTION_CONFIG_KEY = "functionConfig";
    private static final String FUNCTION_EXPR_KEY = "functionExpr";
    private static final String PARTITIONS_KEY = "partitions";

    // DO NOT CHANGE: for backward-compatibility
    private static final String LEGACY_PARTITIONS_KEY = "partitionRanges";
    private static final char LEGACY_PARTITION_DELIMITER = ',';

    @Override
    public ColumnPartitionMetadata deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode jsonMetadata = p.getCodec().readTree(p);
      IntSet partitions;
      JsonNode jsonPartitions = jsonMetadata.get(PARTITIONS_KEY);
      if (jsonPartitions != null) {
        // Integer format: "partitions":[0,1,5]
        partitions = new IntOpenHashSet(jsonPartitions.size());
        for (JsonNode jsonPartition : jsonPartitions) {
          partitions.add(jsonPartition.asInt());
        }
      } else {
        // Legacy format: "partitionRanges":"[0 1],[5 5]"
        partitions = new IntOpenHashSet();
        String partitionRanges = jsonMetadata.get(LEGACY_PARTITIONS_KEY).asText();
        for (String partitionRange : StringUtils.split(partitionRanges, LEGACY_PARTITION_DELIMITER)) {
          addRangeToPartitions(partitionRange, partitions);
        }
      }
      Map<String, String> functionConfig = null;
      if (jsonMetadata.has(FUNCTION_CONFIG_KEY)) {
        functionConfig = JsonUtils.jsonNodeToObject(jsonMetadata.get(FUNCTION_CONFIG_KEY), new TypeReference<>() {
        });
      }

      JsonNode functionNameNode = jsonMetadata.get(FUNCTION_NAME_KEY);
      JsonNode functionExprNode = jsonMetadata.get(FUNCTION_EXPR_KEY);
      // numPartitions is mandatory in segment partition metadata. Surface a clear error if absent rather than
      // letting the caller's catch handler log an opaque NPE.
      JsonNode numPartitionsNode = jsonMetadata.get(NUM_PARTITIONS_KEY);
      if (numPartitionsNode == null || numPartitionsNode.isNull()) {
        throw new IllegalArgumentException(
            "'" + NUM_PARTITIONS_KEY + "' is required in segment partition metadata");
      }
      return new ColumnPartitionMetadata(readOptionalText(functionNameNode),
          numPartitionsNode.asInt(), partitions, functionConfig, readOptionalText(functionExprNode));
    }

    @Nullable
    private static String readOptionalText(@Nullable JsonNode node) {
      return node != null && !node.isNull() ? normalizeOptionalText(node.asText()) : null;
    }
  }
}
