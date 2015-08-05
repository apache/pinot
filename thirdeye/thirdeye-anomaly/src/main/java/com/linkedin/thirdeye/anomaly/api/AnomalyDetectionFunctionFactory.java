package com.linkedin.thirdeye.anomaly.api;

import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 * When supporting new modes of anomaly detection.
 */
public abstract class AnomalyDetectionFunctionFactory {

  /**
   * @return
   *  The class representing the function's representation in the anomaly database.
   */
  public abstract Class<? extends FunctionTableRow> getFunctionRowClass();

  /**
   * @return
   *  A compiled AnomalyDetectionFunction.
   * @throws IllegalFunctionException
   *  When illegal configuration parameters are passed to init.
   * @throws Exception
   */
  public abstract AnomalyDetectionFunction getFunction(StarTreeConfig starTreeConfig, AnomalyDatabaseConfig dbconfig,
      FunctionTableRow functionTableRow) throws IllegalFunctionException, Exception;

}
