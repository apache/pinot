package com.linkedin.thirdeye.client.diffsummary.teradata;

import com.google.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryTera {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

  private Connection con;

  private static final String BASE_QUERY_1 = "SELECT %s, SUM(METRIC) AS METRIC FROM %s WHERE DATETIME_FLAG BETWEEN ? and ? GROUP BY %s;";
  private static final String BASE_QUERY_2 = "SELECT SUM(METRIC) AS METRIC FROM %s WHERE DATETIME_FLAG BETWEEN ? and ?;";
  private static final String META_QUERY = "SELECT top 1 * FROM %s";

  @Inject
  public QueryTera(@TeraDs DataSource ds) {
    try {
      con = ds.getConnection();
    } catch (SQLException e) {
      LOG.error("Fail to connect to Teradata");
      throw new IllegalStateException(e);
    }
  }

  private boolean _validateTable(String tableName) {
    return !tableName.contains(";") || !tableName.contains(" ");
  }

  public List<String> getColumnNames(String tableName) {
    if (!_validateTable(tableName)) {
      LOG.warn(String.format("Check table names  %s", tableName));
    }

    List<String> colNames = new ArrayList<>();
    PreparedStatement _preparedStatement = null;
    try {
      _preparedStatement = con.prepareStatement(String.format(META_QUERY, tableName));
      ResultSet rset = _preparedStatement.executeQuery();
      ResultSetMetaData rsmd = rset.getMetaData();
      int colCount = rsmd.getColumnCount();
      for (int i = 1; i <= colCount; i++) {
        colNames.add(rsmd.getColumnName(i));
      }
    } catch (SQLException ex) {
      LOG.error(String.format("Fail to get the column names of table %s", tableName));
      throw new IllegalStateException(ex);
    } finally {
      try {
        if (_preparedStatement != null) {
          _preparedStatement.close();
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }

    return colNames;
  }

  public Map<List<String>, Double> groupBy(List<String> dimensions, String tableName, DateTime start, DateTime end) {
    if (!_validateTable(tableName)) {
      LOG.warn(String.format("Check table names  %s", tableName));
    }

    String BASE_QUERY;
    int numDim = 0;
    if (dimensions == null || dimensions.isEmpty()) {
      BASE_QUERY = String.format(BASE_QUERY_2, tableName);
    } else {
      numDim = dimensions.size();
      String groupByString = dimensions.stream().collect(Collectors.joining(","));
      BASE_QUERY = String.format(BASE_QUERY_1, groupByString, tableName, groupByString);
    }

    Map<List<String>, Double> groupByResult = new HashMap<>();
    PreparedStatement _preparedStatement = null;

    try {
      _preparedStatement = con.prepareStatement(BASE_QUERY);
      _preparedStatement.setTimestamp(1, new Timestamp(start.getMillis()));
      _preparedStatement.setTimestamp(2, new Timestamp(end.getMillis()));
      ResultSet rset = _preparedStatement.executeQuery();
      ResultSetMetaData rsmd = rset.getMetaData();
      while (rset.next()) {
        List<String> tmp = new ArrayList<>();
        Double metric = 0.;
        for (int i = 1; i <= numDim+1; i++) {
          String colName = rsmd.getColumnName(i);
          Class columnClass = Class.forName(rsmd.getColumnClassName(i));
          Object dimValue = columnClass.cast(rset.getObject(i));
          if (colName.toUpperCase().equals("METRIC")) {
            metric = Double.valueOf(dimValue.toString());
          } else {
            tmp.add(dimValue.toString());
          }
        }
        groupByResult.put(tmp, metric);
      }
      _preparedStatement.close();
    } catch (SQLException e) {
      LOG.error("Fail to execute query");
      throw new IllegalStateException(e);
    } catch (ClassNotFoundException e) {
      LOG.error("Fail to parse a column");
      throw new IllegalStateException(e);
    } finally {
      try {
        if (_preparedStatement != null) {
          _preparedStatement.close();
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }

    return groupByResult;
  }

  public void closeConnection() {
    try {
      if (!(con == null) && !con.isClosed()) {
        con.close();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
