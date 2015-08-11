package com.linkedin.thirdeye.reporting.api;

public class DBSpec {

  private String url;
  private String user;
  private String password;
  private String functionTableName;
  private String anomalyTableName;
  private boolean useConnectionPool;

  public DBSpec() {

  }

  public DBSpec(String url, String user, String password, String functionTableName, String anomalyTableName,
      boolean useConnectionPool) {

    this.url = url;
    this.user = user;
    this.password = password;
    this.functionTableName = functionTableName;
    this.anomalyTableName = anomalyTableName;
    this.useConnectionPool = useConnectionPool;
  }


  public String getUrl() {
    return url;
  }


  public void setUrl(String url) {
    this.url = url;
  }


  public String getUser() {
    return user;
  }


  public void setUser(String user) {
    this.user = user;
  }


  public String getPassword() {
    return password;
  }


  public void setPassword(String password) {
    this.password = password;
  }


  public String getFunctionTableName() {
    return functionTableName;
  }


  public void setFunctionTableName(String functionTableName) {
    this.functionTableName = functionTableName;
  }


  public String getAnomalyTableName() {
    return anomalyTableName;
  }


  public void setAnomalyTableName(String anomalyTableName) {
    this.anomalyTableName = anomalyTableName;
  }


  public boolean isUseConnectionPool() {
    return useConnectionPool;
  }


  public void setUseConnectionPool(boolean useConnectionPool) {
    this.useConnectionPool = useConnectionPool;
  }


}
