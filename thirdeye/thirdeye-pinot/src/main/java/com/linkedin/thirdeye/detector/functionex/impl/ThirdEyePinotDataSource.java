package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import com.linkedin.thirdeye.detector.functionex.dataframe.Series;
import com.linkedin.thirdeye.detector.functionex.dataframe.StringSeries;
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

    if(resultSetGroup.getResultSetCount() <= 0)
      throw new IllegalArgumentException(String.format("Query did not return any results: '%s'", query));

    if(resultSetGroup.getResultSetCount() > 1)
      throw new IllegalArgumentException(String.format("Query returned multiple results: '%s'", query));

    ResultSet resultSet = resultSetGroup.getResultSet(0);
    LOG.debug("col_count={} row_count={} group_key_len={}",
        resultSet.getColumnCount(), resultSet.getRowCount(), resultSet.getGroupKeyLength());

    DataFrame df = new DataFrame(resultSet.getRowCount());

    // TODO conditions not necessarily safe
    if(resultSet.getGroupKeyLength() == 0 && resultSet.getRowCount() == 1 && resultSet.getColumnCount() == 1) {
      // aggregation result
      LOG.debug("Mapping AggregationResult to DataFrame");

      String function = resultSet.getColumnName(0);
      String value = resultSet.getString(0, 0);
      df.addSeries(function, DataFrame.toSeries(new String[] { value }));

    } else if(resultSet.getGroupKeyLength() == 0) {
      // selection result
      LOG.debug("Mapping SelectionResult to DataFrame");

      for (int i = 0; i < resultSet.getColumnCount(); i++) {
        df.addSeries(resultSet.getColumnName(i), makeSelectionSeries(resultSet, i));
      }

    } else if(resultSet.getGroupKeyLength() > 0 && resultSet.getColumnCount() == 1) {
      // groupby result
      LOG.debug("Mapping GroupByResult to DataFrame");

      String function = resultSet.getColumnName(0);
      df.addSeries(function, makeGroupByValueSeries(resultSet));
      df.addSeries("group", makeGroupByGroupSeries(resultSet));

    } else {
      // defensive
      throw new IllegalStateException("Could not determine DataFrame shape from output");
    }

    return df;
  }

  static String extractTableName(String query) {
    Matcher m = PATTERN_TABLE.matcher(query);
    if(!m.find())
      throw new IllegalArgumentException(String.format("Could not get table name from query '%s'", query));
    return m.group(1).toLowerCase();
  }

  static Series makeSelectionSeries(ResultSet resultSet, int colIndex) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return new StringSeries(new String[0]);

    //DataFrame.SeriesType type = inferType(resultSet.getString(0, colIndex));

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getString(i, colIndex);
    }

    return DataFrame.toSeries(values);
  }

  static Series makeGroupByValueSeries(ResultSet resultSet) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return new StringSeries(new String[0]);

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getString(i, 0);
    }

    return DataFrame.toSeries(values);
  }

  static Series makeGroupByGroupSeries(ResultSet resultSet) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return new StringSeries(new String[0]);

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = makeGroupKeyString(resultSet, i);
    }

    return DataFrame.toSeries(values);
  }

  static String makeGroupKeyString(ResultSet resultSet, int rowIndex) {
    int len = resultSet.getGroupKeyLength();
    String[] s = new String[len];
    for(int i=0; i<len; i++)
      s[i] = resultSet.getGroupKeyString(rowIndex, i);
    return String.join("|", s);
  }
}
