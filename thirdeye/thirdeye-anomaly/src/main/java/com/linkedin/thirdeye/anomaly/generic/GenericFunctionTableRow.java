package com.linkedin.thirdeye.anomaly.generic;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;

/**
 *
 */
public final class GenericFunctionTableRow extends FunctionTableRow {

  /** URL of the jar */
  private String jarUrl;

  /** Name of the function class to load */
  private String className;

  /** Initialization properties for function */
  private String functionProperties;

  public String getClassName() {
    return className;
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public String getFunctionProperties() {
    return functionProperties;
  }

  public void subclassInit(ResultSet rs) throws SQLException {
    jarUrl = rs.getString("jar_url");
    className = rs.getString("class_name");
    functionProperties = rs.getString("properties");
  }
}
