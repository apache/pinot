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
   * Represents a single watermark with its partition, sequence, and offset.
   */
  public static class Watermark {
    private long _partitionGroupId;
    private long _sequenceNumber;
    private long _offset;

    /**
     * The @JsonCreator annotation tells Jackson to use this constructor to create
     * a WaterMark instance from a JSON object. The @JsonProperty annotations
     * map the keys in the JSON object to the constructor parameters.
     *
     * @param partitionGroupId The ID of the partition group.
     * @param sequenceNumber The sequence number of the watermark.
     * @param offset The offset of the watermark.
     */
    @JsonCreator
    public Watermark(@JsonProperty("partitionGroupId") long partitionGroupId,
        @JsonProperty("sequenceNumber") long sequenceNumber, @JsonProperty("offset") long offset) {
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
    public long getPartitionGroupId() {
      return _partitionGroupId;
    }

    /**
     * Gets the sequence number.
     *
     * @return The sequence number.
     */
    @JsonGetter("sequenceNumber")
    public long getSequenceNumber() {
      return _sequenceNumber;
    }

    /**
     * Gets the offset.
     *
     * @return The offset.
     */
    @JsonGetter("offset")
    public long getOffset() {
      return _offset;
    }
  }
}
