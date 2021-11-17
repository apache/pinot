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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import java.util.Arrays;
import java.util.Comparator;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.joda.time.format.DateTimeFormatter;


public class StringDateTimeColumnPreIndexStatsCollector extends StringColumnPreIndexStatsCollector {
  private final DateTimeFormatter _dateTimeFormatter;

  public StringDateTimeColumnPreIndexStatsCollector(String column, DateTimeFieldSpec fieldSpec,
      StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    _dateTimeFormatter = new DateTimeFormatSpec(fieldSpec.getFormat()).getDateTimeFormatter();
  }

  @Override
  public void seal() {
    _sortedValues = _values.toArray(new String[0]);
    Arrays.sort(_sortedValues, new StringDateTimeComparator(_dateTimeFormatter));
    _sealed = true;
  }

  private static class StringDateTimeComparator implements Comparator<String> {
    private final DateTimeFormatter _dateTimeFormatter;

    private StringDateTimeComparator(DateTimeFormatter dateTimeFormatter) {
      _dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public int compare(String l, String r) {
      return _dateTimeFormatter.parseDateTime(l).compareTo(_dateTimeFormatter.parseDateTime(r));
    }
  }
}
