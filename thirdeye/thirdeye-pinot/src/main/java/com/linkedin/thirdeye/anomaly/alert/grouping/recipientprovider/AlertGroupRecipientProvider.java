package com.linkedin.thirdeye.anomaly.alert.grouping.recipientprovider;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.Map;

/**
 * Given a dimension map, a provider returns a list of email recipients (separated by commas) for that dimension.
 */
public interface AlertGroupRecipientProvider {

  /**
   * Sets the properties of this grouper.
   *
   * @param props the properties for this grouper.
   */
  void setParameters(Map<String, String> props);

  // TODO: Enhance the interface to handle group anomalies for various types of grouping logic, e.g. multi-metric.
  /**
   * Returns a list of email recipients (separated by commas) for the given dimension.
   *
   * @param dimensions the dimension to look for the recipients.
   *
   * @return a list of email recipients (separated by commas).
   */
  String getAlertGroupRecipients(DimensionMap dimensions);
}
