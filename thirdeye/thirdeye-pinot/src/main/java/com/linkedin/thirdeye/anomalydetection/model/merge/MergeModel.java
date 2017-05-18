package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Properties;

public interface MergeModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Returns the properties of this model.
   */
  Properties getProperties();

  /**
   * Computes the information, e.g., weight, score, average current values, average baseline values,
   * etc., of the given anomaly and update the information to the anomaly.
   *
   * @param anomalyDetectionContext the context that provides the trained prediction model for
   *                                update the information of the given (merged) anomaly.
   *
   * @param anomalyToUpdated the anomaly of which the information is updated.
   */
  void update(AnomalyDetectionContext anomalyDetectionContext, MergedAnomalyResultDTO anomalyToUpdated);

  /**
   * Justify if two anomalies can be merged based on properties
   * e.g. anomalies generated from different anomaly function configurations should not be merged
   * @param anomaly1 the first anomaly
   * @param anomaly2 the second anomaly
   * @return True if two anomalies are eligible to be merged
   */
  Boolean isMergeable(MergedAnomalyResultDTO anomaly1, MergedAnomalyResultDTO anomaly2);

}
