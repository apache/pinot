package com.linkedin.thirdeye.anomaly.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;

/**
 *
 */
public class FunctionTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionTable.class);

  public static <T extends FunctionTableRow> List<T> selectRows(AnomalyDatabaseConfig dbConfig,
      Class<T> rowClass) throws InstantiationException, IllegalAccessException {

    List<T> functionTableRows = new LinkedList<>();

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = dbConfig.getConnection();
      stmt = conn.createStatement();
      rs = stmt.executeQuery(buildFunctionTableSelectStmt(dbConfig));

      while (rs.next()) {
        T row = rowClass.newInstance();
        row.init(rs);
        functionTableRows.add(row);
      }

      return functionTableRows;

    } catch (SQLException e) {
      LOGGER.error("load rule sql exception", e);
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

  /**
   * @param dbConfig
   * @return
   */
  private static String buildFunctionTableSelectStmt(AnomalyDatabaseConfig dbConfig) {
    String formatString = ResourceUtils.getResourceAsString("database/function/select-function-table-template.sql");
    return String.format(formatString, dbConfig.getFunctionTableName());
  }

}
