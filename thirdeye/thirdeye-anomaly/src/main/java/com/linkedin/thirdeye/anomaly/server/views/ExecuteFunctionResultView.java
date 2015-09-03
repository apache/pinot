package com.linkedin.thirdeye.anomaly.server.views;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.server.api.AnomalyResultPrintable;

/**
 *
 */
public class ExecuteFunctionResultView extends AbstractView {

  private final int functionId;
  private final String functionName;
  private final String functionDescription;
  private final List<AnomalyResultPrintable> anomalies;

  public ExecuteFunctionResultView(
      String database,
      String functionTableName,
      String collection,
      int functionId,
      String functionName,
      String functionDescription,
      List<AnomalyResult> anomalies) throws IOException {
    super("execute-function-result-view.ftl", database, functionTableName, collection);
    this.anomalies = new ArrayList<AnomalyResultPrintable>(anomalies.size());
    for (AnomalyResult ar : anomalies) {
      this.anomalies.add(new AnomalyResultPrintable(ar));
    }
    this.functionId = functionId;
    this.functionName = functionName;
    this.functionDescription = functionDescription;
  }

  public List<AnomalyResultPrintable> getAnomalies() {
    return anomalies;
  }

  public int getFunctionId() {
    return functionId;
  }

  public String getFunctionDescription() {
    return functionDescription;
  }

  public String getFunctionName() {
    return functionName;
  }

}
