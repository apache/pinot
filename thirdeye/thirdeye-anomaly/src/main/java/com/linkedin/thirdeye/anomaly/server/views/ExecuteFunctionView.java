package com.linkedin.thirdeye.anomaly.server.views;

import java.util.List;

/**
 *
 */
public class ExecuteFunctionView extends AbstractView {

  private final int functionId;
  private final List<String> dimensionNames;
  private final String responseUrl;

  public ExecuteFunctionView(
      String database,
      String functionTable,
      String collection,
      int functionId,
      List<String> dimensionNames,
      String responseUrl) {
    super("execute-function-view.ftl", database, functionTable, collection);
    this.functionId = functionId;
    this.responseUrl = responseUrl;
    this.dimensionNames = dimensionNames;
  }

  public int getFunctionId() {
    return functionId;
  }

  public String getResponseUrl() {
    return responseUrl;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

}
