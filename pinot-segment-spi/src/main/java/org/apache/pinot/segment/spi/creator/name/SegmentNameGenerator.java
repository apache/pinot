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
package org.apache.pinot.segment.spi.creator.name;

import com.google.common.base.Joiner;
import java.io.Serializable;
import javax.annotation.Nullable;


/**
 * Interface for segment name generator based on the segment sequence id and time range.
 */
public interface SegmentNameGenerator extends Serializable {
  Joiner JOINER = Joiner.on('_').skipNulls();

  /**
   * Generates the segment name.
   *
   * @param sequenceId Segment sequence id (negative value means INVALID)
   * @param minTimeValue Minimum time value
   * @param maxTimeValue Maximum time value
   * @return Segment name generated
   */
  String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue);

}
