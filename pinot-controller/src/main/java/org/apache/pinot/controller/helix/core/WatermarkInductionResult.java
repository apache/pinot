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
package org.apache.pinot.controller.helix.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


/**
 * Represents the result of a watermark induction process, containing a list of watermarks.
 */
public class WatermarkInductionResult {

  private List<Watermark> _watermarks;

  /**
   * The @JsonCreator annotation marks this constructor to be used for deserializing
   * a JSON array back into a WaterMarks object.
   *
   * @param watermarks The list of watermarks.
   */
  @JsonCreator
  public WatermarkInductionResult(@JsonProperty("watermarks") List<Watermark> watermarks) {
    _watermarks = watermarks;
  }

  /**
   * Gets the list of watermarks.
   *
   * @return The list of watermarks.
   */
  @JsonGetter("watermarks")
  public List<Watermark> getWatermarks() {
    return _watermarks;
  }

  /**
   * Represents a single watermark with its partitionGroupId, sequence, and offset.
   */
  public static class Watermark {
    private int _partitionGroupId;
    private int _topicId;
    private int _sequenceNumber;
    private long _offset;

    /**
     * The @JsonCreator annotation tells Jackson to use this constructor to create
     * a WaterMark instance from a JSON object. The @JsonProperty annotations
     * map the keys in the JSON object to the constructor parameters.
     *
     * @param partitionGroupId The stream partition id of the partition group, decoded from any legacy
     *                         padded/composite id. Defaults topicId to 0 (single-topic tables).
     * @param sequenceNumber The segment sequence number of the consuming segment.
      * @param offset The first Kafka offset whose corresponding record has not yet sealed in Pinot
     */
    public Watermark(int partitionGroupId, int sequenceNumber, long offset) {
      this(partitionGroupId, 0, sequenceNumber, offset);
    }

    /**
     * The @JsonCreator annotation tells Jackson to use this constructor to create
     * a WaterMark instance from a JSON object. The @JsonProperty annotations
     * map the keys in the JSON object to the constructor parameters.
     *
     * @param partitionGroupId The stream partition id of the partition group (i.e. the partition id within its
     *                         topic/stream config), not the raw/composite id used by legacy padded encodings.
     * @param topicId The id of the topic (stream config) this partition group belongs to, for tables with
     *                multiple topics.
     * @param sequenceNumber The segment sequence number of the consuming segment.
      * @param offset The first Kafka offset whose corresponding record has not yet sealed in Pinot
     */
    @JsonCreator
    public Watermark(@JsonProperty("partitionGroupId") int partitionGroupId, @JsonProperty("topicId") int topicId,
        @JsonProperty("sequenceNumber") int sequenceNumber, @JsonProperty("offset") long offset) {
      _partitionGroupId = partitionGroupId;
      _topicId = topicId;
      _sequenceNumber = sequenceNumber;
      _offset = offset;
    }

    /**
     * Gets the stream partition id of the partition group.
     *
     * @return The stream partition id.
     */
    @JsonGetter("partitionGroupId")
    public int getPartitionGroupId() {
      return _partitionGroupId;
    }

    /**
     * Gets the topic ID (stream config index) this partition group belongs to.
     *
     * @return The topic ID.
     */
    @JsonGetter("topicId")
    public int getTopicId() {
      return _topicId;
    }

    /**
     * Gets the segment sequence number of the most recent consuming segment.
     *
     * @return The segment sequence number.
     */
    @JsonGetter("sequenceNumber")
    public int getSequenceNumber() {
      return _sequenceNumber;
    }

    /**
     * The high-watermark of the Kafka offsets. Any Kafka record with an offset greater than or equal to this
     * value has not yet been sealed.
     *
     * @return The offset.
     */
    @JsonGetter("offset")
    public long getOffset() {
      return _offset;
    }
  }
}
