package com.linkedin.thirdeye.anomaly.generic;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;

/**
 *
 */
public final class GenericFunctionTableRow extends FunctionTableRow {

  private String jarUrl;
  private String className;
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
