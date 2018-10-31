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

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;


public class DefaultSegmentNameGenerator implements SegmentNameGenerator {
  private final String _segmentName;
  private final String _timeColumnName;
  private final String _tableName;
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

  /**
   *
   * The sequenceId is used for numbering segments while the postfix is used to append a string to the segment name.
   *
   * Some examples:
   *
   * If the time column name exists, _segmentNamePostfix = "postfix", and _sequenceId = 1, the segment name would be
   * tableName_minDate_maxDate_postfix_1
   *
   * If there is no time column, _segmentNamePostfix = "postfix" and _sequenceId = 1, the segment name would be
   * tableName_postfix_1
   *
   * If there is no time column, no postfix, and no sequence id, the segment name would be
   * tableName
   *
   * If there is no time column, a postfix, and no sequence id, the segment name would be
   * tableName_postfix
   *
   * If there is no time column, no postfix, and a sequence id, the segment name would be
   * tableName_sequenceId
   *
   * @param statsCollector
   * @return
   * @throws Exception
   */
  @Override
  public String generateSegmentName(ColumnStatistics statsCollector) throws Exception {
    if (_segmentName != null) {
      return _segmentName;
    }

    String segmentName;

    if (_timeColumnName != null && _timeColumnName.length() > 0) {
      final Object minTimeValue = statsCollector.getMinValue();
      final Object maxTimeValue = statsCollector.getMaxValue();
      if (_segmentNamePostfix == null) {
        segmentName = buildBasic(_tableName, minTimeValue, maxTimeValue, _sequenceId);
      } else {
        segmentName = buildBasic(_tableName, minTimeValue, maxTimeValue, _sequenceId, _segmentNamePostfix);
      }
    } else {
      if (_segmentNamePostfix == null) {
        segmentName = buildBasic(_tableName, _sequenceId);
      } else {
        segmentName = buildBasic(_tableName, _sequenceId, _segmentNamePostfix);
      }
    }

    return segmentName;
  }

  protected static String buildBasic(String tableName, Object minTimeValue, Object maxTimeValue, int sequenceId, String postfix) {
    if (sequenceId == -1) {
      return StringUtil.join("_", tableName, minTimeValue.toString(), maxTimeValue.toString(), postfix);
    } else {
      return StringUtil.join("_", tableName, minTimeValue.toString(), maxTimeValue.toString(), postfix, Integer.toString(sequenceId));
    }
  }

  protected static String buildBasic(String tableName, Object minTimeValue, Object maxTimeValue, int sequenceId) {
    if (sequenceId == -1) {
      return StringUtil.join("_", tableName, minTimeValue.toString(), maxTimeValue.toString());
    } else {
      return StringUtil.join("_", tableName, minTimeValue.toString(), maxTimeValue.toString(), Integer.toString(sequenceId));
    }
  }

  protected static String buildBasic(String tableName, int sequenceId) {
    if (sequenceId == -1) {
      return tableName;
    } else {
      return StringUtil.join("_", tableName, Integer.toString(sequenceId));
    }
  }

  protected static String buildBasic(String tableName, int sequenceId, String postfix) {
    if (sequenceId == -1) {
      return StringUtil.join("_", tableName, postfix);
    } else {
      return StringUtil.join("_", tableName, postfix, Integer.toString(sequenceId));
    }
  }
}
