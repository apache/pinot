package com.linkedin.thirdeye.anomaly.database;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Class representing the required fields of a function table.
 */
public abstract class FunctionTableRow {

  /** An unique id for the function */
  private int functionId;

  /** Name of the function, e.g., 'percent-change', 'arima', etc. */
  private String functionName;

  /** Human understandable description of the function and what it is to compute. e.g., '20% drop in X' */
  private String functionDescription;

  /** The collection to containing data to apply the function to */
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
