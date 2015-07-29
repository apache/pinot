package com.linkedin.thirdeye.anomaly.database;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Class representing the required fields of a function table.
 */
public abstract class FunctionTableRow {

  private int functionId;
  private String functionName;
  private String functionDescription;
  private String collectionName;

  public int getFunctionId() {
    return functionId;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getFunctionDescription() {
    return functionDescription;
  }

  public String getCollectionName() {
    return collectionName;
  }

  /**
   * Base function table init from result set
   *
   * @param rs
   * @throws SQLException
   */
  public final void init(ResultSet rs) throws SQLException {
    functionId = rs.getInt("id");
    functionName = rs.getString("name");
    functionDescription = rs.getString("description");
    collectionName = rs.getString("collection");
    subclassInit(rs);
  }

  /**
   * Initialize any additional fields in subclass
   *
   * @param rs
   * @throws SQLException
   */
  protected abstract void subclassInit(ResultSet rs) throws SQLException;

}
