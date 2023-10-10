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

import com.google.common.base.Preconditions;
import java.util.UUID;
import javax.annotation.Nullable;


/**
 * Simple segment name generator which does not perform time conversion.
 * <p>
 * The segment name is simply joining the following fields with '_' but ignoring all the {@code null}s.
 * <ul>
 *   <li>Table name</li>
 *   <li>Minimum time value</li>
 *   <li>Maximum time value</li>
 *   <li>Segment name postfix</li>
 *   <li>Sequence id</li>
 * </ul>
 */
@SuppressWarnings("serial")
public class SimpleSegmentNameGenerator implements SegmentNameGenerator {
  private final String _segmentNamePrefix;
  private final String _segmentNamePostfix;
  private final boolean _appendUUIDToSegmentName;
  private final boolean _excludeTimeInSegmentName;

  public SimpleSegmentNameGenerator(String segmentNamePrefix, @Nullable String segmentNamePostfix) {
    this(segmentNamePrefix, segmentNamePostfix, false, false);
  }

  public SimpleSegmentNameGenerator(String segmentNamePrefix, @Nullable String segmentNamePostfix,
      boolean appendUUIDToSegmentName, boolean excludeTimeInSegmentName) {
    Preconditions.checkArgument(segmentNamePrefix != null, "Missing segmentNamePrefix for SimpleSegmentNameGenerator");
    SegmentNameUtils.validatePartialOrFullSegmentName(segmentNamePrefix);
    if (segmentNamePostfix != null) {
      SegmentNameUtils.validatePartialOrFullSegmentName(segmentNamePostfix);
    }
    _segmentNamePrefix = segmentNamePrefix;
    _segmentNamePostfix = segmentNamePostfix;
    _appendUUIDToSegmentName = appendUUIDToSegmentName;
    _excludeTimeInSegmentName = excludeTimeInSegmentName;
  }

  @Override
  public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
    if (_excludeTimeInSegmentName) {
      return JOINER.join(_segmentNamePrefix, _segmentNamePostfix, sequenceId >= 0 ? sequenceId : null,
          _appendUUIDToSegmentName ? UUID.randomUUID().toString() : null);
    } else {
      if (minTimeValue != null) {
        SegmentNameUtils.validatePartialOrFullSegmentName(minTimeValue.toString());
      }
      if (maxTimeValue != null) {
        SegmentNameUtils.validatePartialOrFullSegmentName(maxTimeValue.toString());
      }

      return JOINER.join(_segmentNamePrefix, minTimeValue, maxTimeValue, _segmentNamePostfix,
          sequenceId >= 0 ? sequenceId : null, _appendUUIDToSegmentName ? UUID.randomUUID().toString() : null);
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder =
        new StringBuilder("SimpleSegmentNameGenerator: tableName=").append(_segmentNamePrefix);
    if (_segmentNamePostfix != null) {
      stringBuilder.append(", segmentNamePostfix=").append(_segmentNamePostfix);
    }
    stringBuilder.append(", appendUUIDToSegmentName=").append(_appendUUIDToSegmentName);
    stringBuilder.append(", excludeTimeInSegmentName=").append(_excludeTimeInSegmentName);
    return stringBuilder.toString();
  }
}
