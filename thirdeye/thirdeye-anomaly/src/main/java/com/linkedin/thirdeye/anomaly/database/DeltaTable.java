package com.linkedin.thirdeye.anomaly.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.function.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.util.DimensionKeyMatchTable;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 * Table for thresholds by dimension keys. The column names are the dimension names.
 */
public class DeltaTable {

  /** MySQL limit on primary key size */
  private static final int MYSQL_PRIMARY_KEY_LIMIT = 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaTable.class);

  private static final Joiner COMMA = Joiner.on(',');

  /** */
  private static final String COLUMN_NAME_DELTA = "delta";

  /**
   * Create an empty match table in the database if one does not exist. Assumes all dimensions are equally wide
   * and all columns are strings.
   *
   * @param dbConfig
   * @param starTreeConfig
   * @param deltaTableName
   */
  public static void create(AnomalyDatabaseConfig dbConfig, StarTreeConfig starTreeConfig, String deltaTableName) {
    List<String> dimensionNames = new ArrayList<String>(starTreeConfig.getDimensions().size());
    for (DimensionSpec dimension : starTreeConfig.getDimensions()) {
      dimensionNames.add(dimension.getName());
    }

    int columnWidth = MYSQL_PRIMARY_KEY_LIMIT / starTreeConfig.getDimensions().size();

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(deltaTableName).append("(\n");
    sb.append("delta DOUBLE NOT NULL,\n");
    for (String dimensionName : dimensionNames) {
      sb.append(String.format("%s VARCHAR(%d) NOT NULL DEFAULT \"?\",\n", dimensionName, columnWidth));
    }
    sb.append("PRIMARY KEY(").append(COMMA.join(dimensionNames)).append("));");

    String sql = sb.toString();
    LOGGER.info("creating delta table if not exists - {}", deltaTableName);
    dbConfig.runSQL(sql);
  }

  /**
   * @param dbConfig
   * @param starTreeConfig
   * @param deltaTableName
   * @return
   *  Loads a match table from the database
   * @throws IllegalFunctionException
   */
  public static DimensionKeyMatchTable<Double> load(AnomalyDatabaseConfig dbConfig,
      StarTreeConfig starTreeConfig, String deltaTableName) throws IllegalFunctionException {

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = dbConfig.getConnection();
      stmt = conn.createStatement();
      rs = stmt.executeQuery(buildDeltaTableQuery(deltaTableName));

      DimensionKeyMatchTable<Double> result = new DimensionKeyMatchTable<>(starTreeConfig.getDimensions());
      List<DimensionSpec> dimensions = starTreeConfig.getDimensions();
      while (rs.next()) {
        double delta = rs.getDouble(COLUMN_NAME_DELTA);
        String[] dimensionValues = new String[dimensions.size()];
        for (int i = 0; i < dimensions.size(); i++) {
          dimensionValues[i] = rs.getString(dimensions.get(i).getName());
        }
        result.put(new DimensionKey(dimensionValues), delta);
      }

      return result;
    } catch (SQLException e) {
      LOGGER.error("could not get delta table", e);
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
    throw new IllegalFunctionException("failed to load delta_table : " + deltaTableName);
  }

  /**
   * @param tableName
   * @return
   */
  private static String buildDeltaTableQuery(String tableName) {
    return String.format("SELECT * FROM %s;", tableName);
  }

}
