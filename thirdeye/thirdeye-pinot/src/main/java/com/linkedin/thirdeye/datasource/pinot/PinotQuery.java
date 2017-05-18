package com.linkedin.thirdeye.datasource.pinot;

public class PinotQuery {

  private String pql;
  private String tableName;

  public PinotQuery(String pql, String tableName) {
    this.pql = pql;
    this.tableName = tableName;
  }

  public String getPql() {
    return pql;
  }

  public void setPql(String pql) {
    this.pql = pql;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public int hashCode() {
    return pql.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    PinotQuery that = (PinotQuery) obj;
    return this.pql.equals(that.pql);
  }
}
