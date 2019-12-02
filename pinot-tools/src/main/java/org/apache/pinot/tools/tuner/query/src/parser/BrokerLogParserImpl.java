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
package org.apache.pinot.tools.tuner.query.src.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link QueryParser}, to parse complete lines of broker log text
 */
public class BrokerLogParserImpl implements QueryParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerLogParserImpl.class);

  //Regex to parse Broker Log
  private static final String BROKER_LOG_REGEX =
      "^.*?table:(.+?)(?:_OFFLINE|_REALTIME|_HYBRID|)?, timeMs:(\\d+), docs:(\\d+)/(\\d+).*, entries:(\\d+)/(\\d+),.*, query:(.+)$";

  private static final Pattern COMPILED_PATTERN = Pattern.compile(BROKER_LOG_REGEX);

  private enum GroupNames {
    ALL,
    TABLE_NAME_WITHOUT_TYPE,
    TOTAL_TIME,
    DOCS_SCANNED_AGGREGATE,
    TOTAL_DOCS,
    NUM_ENTRIES_SCANNED_IN_FILTER,
    NUM_ENTRIES_SCANNED_POST_FILTER,
    QUERY
  }

  @Nullable
  @Override
  public AbstractQueryStats parse(String line) {
    try {
      Matcher match = COMPILED_PATTERN.matcher(line);
      LOGGER.trace("Original line: " + line);
      if (match.find()) {
        IndexSuggestQueryStatsImpl ret =
            new IndexSuggestQueryStatsImpl.Builder().setTime(match.group(GroupNames.TOTAL_TIME.ordinal()))
                .setTableNameWithoutType(match.group(GroupNames.TABLE_NAME_WITHOUT_TYPE.ordinal()))
                .setNumEntriesScannedInFilter(match.group(GroupNames.NUM_ENTRIES_SCANNED_IN_FILTER.ordinal()))
                .setNumEntriesScannedPostFilter(match.group(GroupNames.NUM_ENTRIES_SCANNED_POST_FILTER.ordinal()))
                .setQuery(match.group(GroupNames.QUERY.ordinal()))
                .build();
        LOGGER.debug("Parsed line: " + ret.toString());
        return ret;
      }
      return null;
    } catch (Exception e) {
      LOGGER.error("Exception while parsing line {}", line, e);
      return null;
    }
  }
}
