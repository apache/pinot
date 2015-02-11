package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
* Enumeration containing all the meters exposed by the Pinot server.
*
* @author jfim
*/
public enum ServerMeter implements AbstractMetrics.Meter {
  QUERIES("queries", true),
  UNCAUGHT_EXCEPTIONS("exceptions", true),
  RESPONSE_SERIALIZATION_EXCEPTIONS("exceptions", true),
  QUERY_EXECUTION_EXCEPTIONS("exceptions", false);

  private final String meterName;
  private final String unit;
  private final boolean global;

  ServerMeter(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.meterName = Utils.toCamelCase(name().toLowerCase());
  }

  public String getMeterName() {
    return meterName;
  }

  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular table or resource)
   *
   * @return true if the metric is global
   */
  public boolean isGlobal() {
    return global;
  }
}
