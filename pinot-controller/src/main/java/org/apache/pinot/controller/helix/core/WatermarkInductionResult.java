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

  private List<String> _historicalSegments;

  /**
   * The @JsonCreator annotation marks this constructor to be used for deserializing
   * a JSON array back into a WaterMarks object.
   *
   * @param watermarks The list of watermarks.
   */
  @JsonCreator
  public WatermarkInductionResult(@JsonProperty("watermarks") List<Watermark> watermarks,
      @JsonProperty("historicalSegments") List<String> historicalSegments) {
    _watermarks = watermarks;
    _historicalSegments = historicalSegments;
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

  @JsonGetter("historicalSegments")
  public List<String> getHistoricalSegments() {
    return _historicalSegments;
  }

  /**
   * Represents a single watermark with its partitionGroupId, sequence, and offset.
   */
  public static class Watermark {
    private int _partitionGroupId;
    private int _sequenceNumber;
    private long _offset;

    /**
     * The @JsonCreator annotation tells Jackson to use this constructor to create
     * a WaterMark instance from a JSON object. The @JsonProperty annotations
     * map the keys in the JSON object to the constructor parameters.
     *
     * @param partitionGroupId The ID of the partition group.
     * @param sequenceNumber The segment sequence number of the consuming segment.
      * @param offset The first Kafka offset whose corresponding record has not yet sealed in Pinot
     */
    @JsonCreator
    public Watermark(@JsonProperty("partitionGroupId") int partitionGroupId,
        @JsonProperty("sequenceNumber") int sequenceNumber, @JsonProperty("offset") long offset) {
      _partitionGroupId = partitionGroupId;
      _sequenceNumber = sequenceNumber;
      _offset = offset;
    }

    /**
     * Gets the partition group ID.
     *
     * @return The partition group ID.
     */
    @JsonGetter("partitionGroupId")
    public int getPartitionGroupId() {
      return _partitionGroupId;
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
