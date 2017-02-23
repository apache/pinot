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

    if (resultSetGroup.getResultSetCount() <= 0)
      throw new IllegalArgumentException(String.format("Query did not return any results: '%s'", query));

    if (resultSetGroup.getResultSetCount() > 1)
      throw new IllegalArgumentException(String.format("Query returned multiple results: '%s'", query));

    ResultSet resultSet = resultSetGroup.getResultSet(0);
    LOG.debug("col_count={} row_count={}", resultSet.getColumnCount(), resultSet.getRowCount());

    DataFrame df = new DataFrame(resultSet.getRowCount());

    // TODO conditions not necessarily safe
    if(resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 0) {
      // empty result
      LOG.debug("Mapping empty to DataFrame");

    } else if(resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 1 && resultSet.getGroupKeyLength() == 0) {
      // aggregation result
      LOG.debug("Mapping AggregationResult to DataFrame");

      String function = resultSet.getColumnName(0);
      String value = resultSet.getString(0, 0);
      df.addSeries(function, DataFrame.toSeries(new String[] { value }));

    } else if(resultSet.getColumnCount() == 1 && resultSet.getGroupKeyLength() > 0) {
      // groupby result
      LOG.debug("Mapping GroupByResult to DataFrame");

      String function = resultSet.getColumnName(0);
      df.addSeries(function, makeGroupByValueSeries(resultSet));
      for(int i=0; i<resultSet.getGroupKeyLength(); i++) {
        String groupKey = resultSet.getGroupKeyColumnName(i);
        df.addSeries(groupKey, makeGroupByGroupSeries(resultSet, i));
      }

    } else if(resultSet.getColumnCount() >= 1 && resultSet.getGroupKeyLength() == 0) {
      // selection result
      LOG.debug("Mapping SelectionResult to DataFrame");

      for (int i = 0; i < resultSet.getColumnCount(); i++) {
        df.addSeries(resultSet.getColumnName(i), makeSelectionSeries(resultSet, i));
      }

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

  static Series makeGroupByGroupSeries(ResultSet resultSet, int keyIndex) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return new StringSeries(new String[0]);

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getGroupKeyString(i, keyIndex);
    }

    return DataFrame.toSeries(values);
  }
}
