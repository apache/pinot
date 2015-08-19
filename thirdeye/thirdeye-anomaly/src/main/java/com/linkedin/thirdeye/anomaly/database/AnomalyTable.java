package com.linkedin.thirdeye.anomaly.database;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.ResultProperties;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class AnomalyTable {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyTable.class);

  private static final Joiner COMMA = Joiner.on(',');
  private static final Joiner AND = Joiner.on(" AND ");

  /**
   * @param dbConfig
   * @param collection
   *  Collection conatining anomalies
   * @param functionName
   *  SQL pattern for function name to match
   * @param functionDescription
   *  SQL pattern for description to match
   * @param metrics
   *  The list of metrics used
   * @param topLevelOnly
   *  Only anomalies with all * dimensions
   * @param orderBy
   *  Components of orderBy clause
   * @param startTimeWindow
   * @param endTimeWindow
   * @return
   *  List of rows based on query produced
   * @throws SQLException
   */
  public static List<AnomalyTableRow> selectRows(
      AnomalyDatabaseConfig dbConfig,
      String collection,
      String functionName,
      String functionDescription,
      Set<String> metrics,
      boolean topLevelOnly,
      List<String> orderBy,
      long startTimeWindow,
      long endTimeWindow) throws SQLException {
    return selectRows(dbConfig, collection, null, functionName, functionDescription, metrics, topLevelOnly,
        orderBy, new TimeRange(startTimeWindow, endTimeWindow));
  }

  public static List<AnomalyTableRow> selectRows(AnomalyDatabaseConfig dbConfig, int functionId, TimeRange timeRange)
      throws SQLException {
    List<String> orderBy = Arrays.asList(new String[]{"time_window"});
    return selectRows(dbConfig, null, functionId, null, null, null, false, orderBy, null);
  }

  public static List<AnomalyTableRow> selectRows(
      AnomalyDatabaseConfig dbConfig,
        String collection,
        Integer functionId,
        String functionName,
        String functionDescription,
        Set<String> metrics,
        boolean topLevelOnly,
        List<String> orderBy,
        TimeRange timeRange) throws SQLException {

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      String sql = buildAnomalyTableSelectStatement(dbConfig, functionId, functionName,
          functionDescription, collection, topLevelOnly, orderBy, timeRange);

      conn = dbConfig.getConnection();
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);

      List<AnomalyTableRow> results = new LinkedList<AnomalyTableRow>();

      while (rs.next()) {
        AnomalyTableRow row = new AnomalyTableRow();
        row.setId(rs.getInt("id"));
        row.setFunctionTable(rs.getString("function_table"));
        row.setFunctionId(rs.getInt("function_id"));
        row.setFunctionName(rs.getString("function_name"));
        row.setFunctionDescription(rs.getString("function_description"));
        row.setCollection(rs.getString("collection"));
        row.setTimeWindow(rs.getLong("time_window"));
        row.setNonStarCount(rs.getInt("non_star_count"));
        row.setDimensions(rs.getString("dimensions"));
        row.setDimensionsContribution(rs.getDouble("dimensions_contribution"));
        row.setMetrics(deserializeMetrics(rs.getString("metrics")));
        row.setAnomalyScore(rs.getDouble("anomaly_score"));
        row.setAnomalyVolume(rs.getDouble("anomaly_volume"));

        ResultProperties properties = new ResultProperties();
        try {
          properties.load(new StringReader(rs.getString("properties")));
        } catch (IOException e) {
          LOGGER.warn("unable to deserialize anomaly result properties", e);
          properties = null;
        }

        row.setProperties(properties);

        // filter on metrics
        if (metrics != null) {
          if (row.getMetrics() != null && row.getMetrics().size() == metrics.size()
            && metrics.containsAll(row.getMetrics())) {
            results.add(row);
          }
        } else {
          results.add(row);
        }
      }

      return results;

    } catch (SQLException e) {
      LOGGER.error("there was a problem retrieving rows", e);
      throw e;

    } finally {
      try {
        if (conn != null) {
          conn.close();
        }
        if (stmt != null) {
          stmt.close();
        }
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException e) {
        LOGGER.error("close exception", e);
      }
    }
  }

  /**
   * Create anomaly table if it does not exist
   *
   * @param dbConfig
   * @throws IOException
   */
  public static void createTable(AnomalyDatabaseConfig dbConfig) throws IOException {
    dbConfig.runSQL(buildAnomalyTableCreateStmt(dbConfig.getAnomalyTableName(), dbConfig.getFunctionTableName()));
  }

  /**
   * Adds the row to the anomaly table referenced in dbConfig
   *
   * @param dbConfig
   * @param row
   * @throws IOException
   */
  public static void insertRow(AnomalyDatabaseConfig dbConfig, AnomalyTableRow row) throws IOException {
    Connection conn = null;
    PreparedStatement preparedStmt = null;

    try {
      conn = dbConfig.getConnection();

      preparedStmt = conn.prepareStatement(buildAnomlayTableInsertStmt(dbConfig.getAnomalyTableName()));
      preparedStmt.setString(1, row.getFunctionTable());
      preparedStmt.setInt(2, row.getFunctionId());
      preparedStmt.setString(3, row.getFunctionDescription());
      preparedStmt.setString(4, row.getFunctionName());
      preparedStmt.setString(5, row.getCollection());
      preparedStmt.setLong(6, row.getTimeWindow());
      preparedStmt.setInt(7, row.getNonStarCount());
      preparedStmt.setString(8, row.getDimensions());
      preparedStmt.setDouble(9, row.getDimensionsContribution());
      preparedStmt.setString(10, serializeMetrics(row.getMetrics()));
      preparedStmt.setDouble(11, row.getAnomalyScore());
      preparedStmt.setDouble(12, row.getAnomalyVolume());

      StringWriter writer = new StringWriter();
      row.getProperties().store(new PrintWriter(writer), "ResultProperties");

      preparedStmt.setString(13, writer.getBuffer().toString());

      preparedStmt.executeUpdate();
    } catch (SQLException | JsonProcessingException e) {
      LOGGER.error("unable to insert row", e);
    } finally {
      try {
        if (conn != null) {
          conn.close();
        }
        if (preparedStmt != null) {
          preparedStmt.close();
        }
      } catch (SQLException e) {
        LOGGER.error("close exception", e);
      }
    }

  }

  /**
   * @param dbconfig
   * @param functionName
   * @param functionDescription
   * @param collection
   * @param metric
   * @param topLevelOnly
   * @param startTimeWindow
   * @param endTimeWidnow
   * @return
   * @throws JsonProcessingException
   */
  private static String buildAnomalyTableSelectStatement(AnomalyDatabaseConfig dbconfig,
      Integer functionId,
      String functionName,
      String functionDescription,
      String collection,
      boolean topLevelOnly,
      List<String> orderBy,
      TimeRange timeRange)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM ").append(dbconfig.getAnomalyTableName()).append(" WHERE ");

    List<String> whereClause = new LinkedList<>();

    if (dbconfig.getFunctionTableName() != null) {
      whereClause.add("function_table = '" + dbconfig.getFunctionTableName() + "'");
    }

    if (functionId != null) {
      whereClause.add("function_id = '" + functionId + "'");
    }

    if (collection != null) {
      whereClause.add("collection = '" + collection + "'");
    }

    if (functionName != null) {
      whereClause.add("function_name = '" + functionName + "'");
    }

    if (functionDescription != null) {
      whereClause.add("function_description LIKE '" + functionDescription + "'");
    }

    if (topLevelOnly) {
      whereClause.add("non_star_count = 0");
    }

    if (timeRange != null) {
      whereClause.add("time_window BETWEEN '" + timeRange.getStart() + "' AND '" + timeRange.getEnd() + "'");
    }

    sb.append(AND.join(whereClause));

    if (orderBy != null && orderBy.size() > 0) {
      sb.append(" ORDER BY ").append(COMMA.join(orderBy));
    }

    sb.append(";");
    return sb.toString();
  }

  private static String buildAnomalyTableCreateStmt(String anomalyTableName, String ruleTableName) throws IOException {
    String formatString = ResourceUtils.getResourceAsString("database/anomaly/create-anomaly-table-template.sql");
    return String.format(formatString, anomalyTableName, ruleTableName);
  }

  private static String buildAnomlayTableInsertStmt(String tableName) throws IOException {
    String formatString = ResourceUtils.getResourceAsString("database/anomaly/insert-into-anomaly-table-template.sql");
    return String.format(formatString, tableName);
  }

  private static Set<String> deserializeMetrics(String metricsString) {
    ObjectReader reader = OBJECT_MAPPER.reader(Set.class);
    try {
      return reader.readValue(metricsString);
    } catch (IOException e) {
      return null;
    }
  }

  private static String serializeMetrics(Set<String> metrics) throws JsonProcessingException {
    return OBJECT_MAPPER.writer().writeValueAsString(metrics);
  }
}
