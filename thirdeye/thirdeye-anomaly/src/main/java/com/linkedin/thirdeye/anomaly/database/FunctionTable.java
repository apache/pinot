package com.linkedin.thirdeye.anomaly.database;

import java.io.IOException;
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

  /**
   * @param dbConfig
   * @param rowClass
   * @return
   *  A list of rows in the function table referenced in dbConfig
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IOException
   * @throws SQLException
   */
  public static <T extends FunctionTableRow> List<T> selectRows(AnomalyDatabaseConfig dbConfig, Class<T> rowClass,
      String collection) throws InstantiationException, IllegalAccessException, IOException, SQLException {

    List<T> functionTableRows = new LinkedList<>();

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = dbConfig.getConnection();
      stmt = conn.createStatement();
      rs = stmt.executeQuery(buildFunctionTableSelectStmt(dbConfig, collection));

      while (rs.next()) {
        T row = rowClass.newInstance();
        row.init(rs);
        functionTableRows.add(row);
      }

      return functionTableRows;

    } catch (SQLException e) {
      LOGGER.error("load function sql exception", e);
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
        e.printStackTrace();
      }
    }
  }

  /**
   * @param dbConfig
   * @return
   * @throws IOException
   */
  private static String buildFunctionTableSelectStmt(AnomalyDatabaseConfig dbConfig, String collection)
      throws IOException {
    String formatString = ResourceUtils.getResourceAsString("database/function/select-function-table-template.sql");
    return String.format(formatString, dbConfig.getFunctionTableName(), collection);
  }

}
