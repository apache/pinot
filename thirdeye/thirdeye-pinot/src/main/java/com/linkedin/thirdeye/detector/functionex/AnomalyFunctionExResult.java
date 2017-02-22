package com.linkedin.thirdeye.detector.functionex;

import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.ArrayList;
import java.util.List;


public class AnomalyFunctionExResult {
  AnomalyFunctionExContext context;
  List<Anomaly> anomalies = new ArrayList<>();

  public AnomalyFunctionExContext getContext() {
    return context;
  }

  public void setContext(AnomalyFunctionExContext context) {
    this.context = context;
  }

  public List<Anomaly> getAnomalies() {
    return anomalies;
  }

  public void setAnomalies(List<Anomaly> anomalies) {
    this.anomalies = anomalies;
  }

  public void addAnomaly(Anomaly a) {
    this.anomalies.add(a);
  }

  public void addAnomaly(long start, long end, String message) {
    this.anomalies.add(new Anomaly(start, end, message, new DataFrame(new long[] {})));
  }

  public void addAnomaly(long start, long end, String message, DataFrame data) {
    this.anomalies.add(new Anomaly(start, end, message, data));
  }

  public static class Anomaly {
    final long start;
    final long end;
    final String message;
    final DataFrame data;

    public Anomaly(long start, long end, String message, DataFrame data) {
      this.start = start;
      this.end = end;
      this.message = message;
      this.data = data;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    public String getMessage() {
      return message;
    }

    public DataFrame getData() {
      return data;
    }
  }
}
