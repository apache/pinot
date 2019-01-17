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
package org.apache.pinot.common.metadata.segment;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.math.IntRange;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.utils.EqualityUtils;


/**
 * Class for partition related column metadata:
 * <ul>
 *   <li> Partition function.</li>
 *   <li> Number of partitions. </li>
 *   <li> List of partition ranges. </li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnPartitionMetadata extends ColumnPartitionConfig {

  private final List<IntRange> _partitionRanges;

  /**
   * Constructor for the class.
   * @param functionName Name of the partition function.
   * @param numPartitions Number of partitions for this column.
   * @param partitionRanges Partition ranges for the column.
   */
  public ColumnPartitionMetadata(@JsonProperty("functionName") String functionName,
      @JsonProperty("numPartitions") int numPartitions,
      @JsonProperty("partitionRanges") @JsonDeserialize(using = PartitionRangesDeserializer.class) List<IntRange> partitionRanges) {
    super(functionName, numPartitions);
    _partitionRanges = partitionRanges;
  }

  /**
   * Returns the list of partition ranges.
   *
   * @return List of partition ranges.
   */
  @JsonSerialize(using = PartitionRangesSerializer.class)
  public List<IntRange> getPartitionRanges() {
    return _partitionRanges;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ColumnPartitionMetadata that = (ColumnPartitionMetadata) o;
    return super.equals(that) && Objects.equals(_partitionRanges, that._partitionRanges);
  }

  @Override
  public int hashCode() {
    int hashCode = _partitionRanges != null ? _partitionRanges.hashCode() : 0;
    return EqualityUtils.hashCodeOf(super.hashCode(), hashCode);
  }

  /**
   * Custom Json serializer for list of IntRange's.
   */
  public static class PartitionRangesSerializer extends JsonSerializer<List<IntRange>> {

    @Override
    public void serialize(List<IntRange> value, JsonGenerator jsonGenerator, SerializerProvider provider)
        throws IOException {
      jsonGenerator.writeString(ColumnPartitionConfig.rangesToString(value));
    }
  }

  /**
   * Custom Json de-serializer for list of IntRange's.
   */
  public static class PartitionRangesDeserializer extends JsonDeserializer<List<IntRange>> {

    @Override
    public List<IntRange> deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      return ColumnPartitionConfig.rangesFromString(jsonParser.getText());
    }
  }
}
