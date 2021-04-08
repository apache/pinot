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

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGenerator;


/**
 * Fixed segment name generator which always returns the fixed segment name.
 */
public class FixedSegmentNameGenerator implements SegmentNameGenerator {
  private final String _segmentName;

  public FixedSegmentNameGenerator(String segmentName) {
    _segmentName = segmentName;
  }

  @Override
  public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
    return _segmentName;
  }

  @Override
  public String toString() {
    return String.format("FixedSegmentNameGenerator: segmentName=%s", _segmentName);
  }
}
