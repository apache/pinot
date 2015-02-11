package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
* Enumeration containing all the metrics exposed by the Pinot broker.
*
* @author jfim
*/
public enum BrokerMeter implements AbstractMetrics.Meter {
  UNCAUGHT_GET_EXCEPTIONS("exceptions", true),
  UNCAUGHT_POST_EXCEPTIONS("exceptions", true),
  QUERIES("queries", false),
  REQUEST_COMPILATION_EXCEPTIONS("exceptions", true),
  REQUEST_FETCH_EXCEPTIONS("exceptions", false),
  REQUEST_DESERIALIZATION_EXCEPTIONS("exceptions", false),
  DOCUMENTS_SCANNED("documents", false);

  private final String brokerMeterName;
  private final String unit;
  private final boolean global;

  BrokerMeter(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.brokerMeterName = Utils.toCamelCase(name().toLowerCase());
  }

  public String getMeterName() {
    return brokerMeterName;
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
