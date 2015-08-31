package com.linkedin.thirdeye.anomaly.generic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;

/**
 *
 */
public final class GenericFunctionTableRow extends FunctionTableRow {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericFunctionTableRow.class);

  /** URL of the jar */
  private String jarUrl;

  /** Name of the function class to load */
  private String className;

  /** Initialization properties for function */
  private String functionProperties;

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public void setJarUrl(String jarUrl) {
    this.jarUrl = jarUrl;
  }

  public String getFunctionProperties() {
    return functionProperties;
  }

  public void setFunctionProperties(String functionProperties) {
    this.functionProperties = functionProperties;
  }

  public void subclassInit(ResultSet rs) throws SQLException {
    jarUrl = rs.getString("jar_url");
    className = rs.getString("class_name");
    functionProperties = rs.getString("properties");
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.database.FunctionTableRow#insert(com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig)
   */
  @Override
  public void insert(AnomalyDatabaseConfig dbConfig) throws Exception {
    Connection conn = null;
    PreparedStatement preparedStmt = null;

    try {
      conn = dbConfig.getConnection();
      preparedStmt = conn.prepareStatement(String.format(
          ResourceUtils.getResourceAsString("database/generic/insert-function-template.sql"),
          dbConfig.getFunctionTableName()));
      preparedStmt.setString(1, getFunctionName());
      preparedStmt.setString(2, getFunctionDescription());
      preparedStmt.setString(3, getCollectionName());
      preparedStmt.setString(4, getJarUrl());
      preparedStmt.setString(5, getClassName());
      preparedStmt.setString(6, getFunctionProperties());
      preparedStmt.executeUpdate();
    } catch (SQLException e) {
      throw e;
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

}
