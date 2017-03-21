package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyePinotDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {
  static final Logger LOG = LoggerFactory.getLogger(ThirdEyePinotDataSource.class);
  static final Pattern PATTERN_TABLE = Pattern.compile("(?i)\\s+FROM\\s+(\\w+)");

  ThirdEyePinotConnection connection;

  public ThirdEyePinotDataSource(ThirdEyePinotConnection connection) {
    this.connection = connection;
  }

  @Override
  public DataFrame query(String query, AnomalyFunctionExContext context) throws Exception {
    String table = extractTableName(query);

    ResultSetGroup resultSetGroup = this.connection.execute(table, query);

    return DataFrame.fromPinotResult(resultSetGroup);
  }

  static String extractTableName(String query) {
    Matcher m = PATTERN_TABLE.matcher(query);
    if(!m.find())
      throw new IllegalArgumentException(String.format("Could not get table name from query '%s'", query));
    return m.group(1).toLowerCase();
  }
}
