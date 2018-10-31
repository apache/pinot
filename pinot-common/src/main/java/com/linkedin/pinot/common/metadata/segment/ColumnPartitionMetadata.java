/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metadata.segment;

import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang.math.IntRange;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;


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

  @JsonSerialize(using = PartitionRangesSerializer.class)
  @JsonDeserialize(using = PartitionRangesDeserializer.class)
  @JsonProperty("partitionRanges")
  private final List<IntRange> _partitionRanges;

  /**
   * Constructor for the class.
   * @param functionName Name of the partition function.
   * @param numPartitions Number of partitions for this column.
   * @param partitionRanges Partition ranges for the column.
   */
  public ColumnPartitionMetadata(@Nonnull @JsonProperty("functionName") String functionName,
      @JsonProperty("numPartitions") int numPartitions,
      @JsonProperty("partitionRanges") List<IntRange> partitionRanges) {
    super(functionName, numPartitions);
    _partitionRanges = partitionRanges;
  }

  /**
   * Returns the list of partition ranges.
   *
   * @return List of partition ranges.
   */
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
    return super.equals(that) && (_partitionRanges != null ? _partitionRanges.equals(that._partitionRanges)
        : that._partitionRanges == null);
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
