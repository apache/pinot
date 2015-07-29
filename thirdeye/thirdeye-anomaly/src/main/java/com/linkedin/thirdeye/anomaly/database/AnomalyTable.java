package com.linkedin.thirdeye.anomaly.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;

/**
 *
 */
public class AnomalyTable {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyTable.class);

  public static List<AnomalyTableRow> selectRows(AnomalyDatabaseConfig dbConfig, String collection, String metric,
      long startTimeWindow, long endTimeWindow) {
    List<String> metrics = null;
    if (metric != null) {
      metrics = new ArrayList<String>(1);
      metrics.add(metric);
    }
    return selectRows(dbConfig, collection, null, null, metrics, false, startTimeWindow, endTimeWindow);
  }

  public static List<AnomalyTableRow> selectRows(
      AnomalyDatabaseConfig dbConfig,
      String collection,
      String functionName,
      String functionDescription,
      List<String> metrics,
      boolean topLevelOnly,
      long startTimeWindow,
      long endTimeWindow) {
      String sql = buildAnomalyTableSelectStatement(dbConfig, functionName, functionDescription, collection,
          metrics, topLevelOnly, startTimeWindow, endTimeWindow);

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = dbConfig.getConnection();
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);

      List<AnomalyTableRow> results = new LinkedList<AnomalyTableRow>();

      while (rs.next()) {
        AnomalyTableRow row = new AnomalyTableRow();
        row.setId(rs.getInt("id"));
        row.setFunctionId(rs.getInt("function_id"));
        row.setFunctionName(rs.getString("function_name"));
        row.setFunctionDescription(rs.getString("function_description"));
        row.setCollection(rs.getString("collection"));
        row.setTimeWindow(rs.getTimestamp("time_window").getTime());
        row.setNonStarCount(rs.getInt("non_star_count"));
        row.setDimensions(rs.getString("dimension"));
        row.setMetrics(deserializeMetrics(rs.getString("metrics")));
        row.setAnomalyScore(rs.getDouble("anomaly_score"));
        row.setAnomalyVolume(rs.getDouble("anomaly_volume"));
        row.setProperties(rs.getString("properties"));
        results.add(row);
      }

      return results;

    } catch (SQLException e) {
      e.printStackTrace();
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
        e.printStackTrace();
      }
    }
    return null;

  }

  public static void createTable(AnomalyDatabaseConfig dbConfig) {
    dbConfig.runSQL(buildAnomalyTableCreateStmt(dbConfig.getAnomalyTableName(), dbConfig.getFunctionTableName()));
  }

  public static void insertRow(AnomalyDatabaseConfig dbConfig, AnomalyTableRow row) {
    Connection conn = null;
    PreparedStatement preparedStmt = null;

    try {
      conn = dbConfig.getConnection();

      preparedStmt = conn.prepareStatement(buildAnomlayTableInsertStmt(dbConfig.getAnomalyTableName()));
      preparedStmt.setInt(1, row.getFunctionId());
      preparedStmt.setString(2, row.getFunctionDescription());
      preparedStmt.setString(3, row.getFunctionName());
      preparedStmt.setString(4, row.getCollection());

      SimpleDateFormat sdf = new SimpleDateFormat(dbConfig.getDateFormat());
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      String dateString = sdf.format(row.getTimeWindow());
      preparedStmt.setString(5, dateString);

      preparedStmt.setInt(6, row.getNonStarCount());
      preparedStmt.setString(7, row.getDimensions());
      preparedStmt.setString(8, serializeMetrics(row.getMetrics()));
      preparedStmt.setDouble(9, row.getAnomalyScore());
      preparedStmt.setDouble(10, row.getAnomalyVolume());
      preparedStmt.setString(11, row.getProperties());

      preparedStmt.executeUpdate();
    } catch (SQLException e) {
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
   * @param ruleName
   * @param ruleDescription
   * @param collection
   * @param metric
   * @param topLevelOnly
   * @param startTimeWindow
   * @param endTimeWidnow
   * @return
   */
  private static String buildAnomalyTableSelectStatement(AnomalyDatabaseConfig dbconfig, String ruleName,
      String ruleDescription, String collection, List<String> metrics, boolean topLevelOnly, long startTimeWindow,
      long endTimeWidnow) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM ").append(dbconfig.getAnomalyTableName()).append(" WHERE");

    if (collection != null) {
      sb.append(" collection = '" + collection + "' AND");
    }

    if (ruleName != null) {
      sb.append(" function_name = '" + ruleName + "' AND");
    }

    if (ruleDescription != null) {
      sb.append(" function_description LIKE '" + ruleDescription + "' AND");
    }

    if (metrics != null) {
      sb.append(" metrics ='" + serializeMetrics(metrics) + "' AND");
    }

    if (topLevelOnly) {
      sb.append(" non_star_count = 0 AND");
    }

    SimpleDateFormat sdf = new SimpleDateFormat(dbconfig.getDateFormat());
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    sb.append(" time_window BETWEEN '" + sdf.format(startTimeWindow) + "' AND '" + sdf.format(endTimeWidnow)+ "'");

    sb.append(" ORDER BY non_star_count, anomaly_volume DESC, time_window DESC, ABS(anomaly_score) DESC");

    sb.append(";");
    return sb.toString();
  }

  private static String buildAnomalyTableCreateStmt(String anomalyTableName, String ruleTableName) {
    String formatString = ResourceUtils.getResourceAsString("database/anomaly/create-anomaly-table-template.sql");
    return String.format(formatString, anomalyTableName, ruleTableName);
  }

  private static String buildAnomlayTableInsertStmt(String tableName) {
    String formatString = ResourceUtils.getResourceAsString("database/anomaly/insert-into-anomaly-table-template.sql");
    return String.format(formatString, tableName);
  }

  private static List<String> deserializeMetrics(String metricsString) {
    ObjectReader reader = OBJECT_MAPPER.reader(List.class);
    try {
      return reader.readValue(metricsString);
    } catch (IOException e) {
      return null;
    }
  }

  private static String serializeMetrics(List<String> metrics) {
    Collections.sort(metrics);
    return new JSONArray(metrics).toString();
  }
}
