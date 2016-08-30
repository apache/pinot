package com.linkedin.thirdeye.hadoop.wait;

public enum WaitPhaseJobConstants {
  WAIT_UDF_CLASS("wait.udf.class"),
  WAIT_POLL_TIMEOUT("wait.poll.timeout"),
  WAIT_POLL_FREQUENCY("wait.poll.frequency");

  String name;

  WaitPhaseJobConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
