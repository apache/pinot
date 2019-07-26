package org.apache.pinot.tools.tuner.query.src.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerLogParserImpl implements QueryParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerLogParserImpl.class);

  //Regex to parse Broker Log
  private static final String BROKER_LOG_REGEX =
      "^.*?table:(.+?)(?:_OFFLINE|_REALTIME|_HYBRID|)?, timeMs:(\\d+), docs:(\\d+)/(\\d+).*, entries:(\\d+)/(\\d+),.*, query:(.+)$";

  private static final Pattern _compiledPattern = Pattern.compile(BROKER_LOG_REGEX);

  private enum GROUP_NAMES {
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
    Matcher match = _compiledPattern.matcher(line);
    LOGGER.trace("Original line: " + line);
    if (match.find()) {
      IndexSuggestQueryStatsImpl ret =
          new IndexSuggestQueryStatsImpl.Builder().setTime(match.group(GROUP_NAMES.TOTAL_TIME.ordinal()))
              .setTableNameWithoutType(match.group(GROUP_NAMES.TABLE_NAME_WITHOUT_TYPE.ordinal()))
              .setNumEntriesScannedInFilter(match.group(GROUP_NAMES.NUM_ENTRIES_SCANNED_IN_FILTER.ordinal()))
              .setNumEntriesScannedPostFilter(match.group(GROUP_NAMES.NUM_ENTRIES_SCANNED_POST_FILTER.ordinal()))
              .setQuery(match.group(GROUP_NAMES.QUERY.ordinal())).build();
      LOGGER.debug("Parsed line: " + ret.toString());
      return ret;
    }
    return null;
  }
}
