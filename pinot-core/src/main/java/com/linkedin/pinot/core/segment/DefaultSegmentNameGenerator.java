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
package com.linkedin.pinot.core.segment;

import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;


public class DefaultSegmentNameGenerator implements SegmentNameGenerator {
  private final String _segmentName;
  private final String _timeColumnName;
  private final String _tableName;
  /*
    The postfix is used if segments have different granularities while the sequenceId is used for numbering segments.
    For example, you can name your segments as
    tableName_date_daily_0 where "daily" is the segmentNamePostfix and 0 is the sequence id.
   */
  private final String _segmentNamePostfix;
  private final int _sequenceId;
  /**
   * To be used when segment name is pre-decided externally
   * @param segmentName
   */
  public DefaultSegmentNameGenerator(final String segmentName) {
    _segmentName = segmentName;
    _tableName = null;
    _timeColumnName = null;
    _segmentNamePostfix = null;
    _sequenceId = -1;
  }

  /**
   * To be used to derive segmentName
   * @param timeColumnName
   * @param tableName
   * @param segmentNamePostfix
   */
  public DefaultSegmentNameGenerator(String timeColumnName, String tableName, String segmentNamePostfix, int sequenceId) {
    _timeColumnName = timeColumnName;
    _tableName = tableName;
    _segmentNamePostfix = segmentNamePostfix;
    _segmentName = null;
    _sequenceId = sequenceId;
  }

  @Override
  public String getSegmentName(AbstractColumnStatisticsCollector statsCollector) throws Exception {
    if (_segmentName != null) {
      return _segmentName;
    }

    String segmentName;

    if (_timeColumnName != null && _timeColumnName.length() > 0) {
      final Object minTimeValue = statsCollector.getMinValue();
      final Object maxTimeValue = statsCollector.getMaxValue();
      segmentName = SegmentNameBuilder
          .buildBasic(_tableName, minTimeValue, maxTimeValue, _segmentNamePostfix, _sequenceId);
    } else {
      segmentName = SegmentNameBuilder.buildBasic(_tableName, _segmentNamePostfix, _sequenceId);
    }

    return segmentName;
  }
}
