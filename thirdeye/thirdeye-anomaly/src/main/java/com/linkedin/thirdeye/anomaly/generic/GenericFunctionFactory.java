package com.linkedin.thirdeye.anomaly.generic;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.net.URLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionFactory;
import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 *
 */
public class GenericFunctionFactory extends AnomalyDetectionFunctionFactory {

  /** */
  private static final String BUILTIN_FUNCTION_QUALIFIED_NAME_PREFIX = "com.linkedin.thirdeye.anomaly.builtin.";

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericFunctionFactory.class);

  @Override
  public Class<? extends FunctionTableRow> getFunctionRowClass() {
    return GenericFunctionTableRow.class;
  }

  /**
   * Loads a function from an external jar or a built in function if no jar url is specified.
   *
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionFactory#getFunction(com.linkedin.thirdeye.api.StarTreeConfig, com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig, com.linkedin.thirdeye.anomaly.database.FunctionTableRow)
   */
  @Override
  public AnomalyDetectionFunction getFunction(StarTreeConfig starTreeConfig, AnomalyDatabaseConfig dbconfig,
      FunctionTableRow functionTableRow) throws IllegalFunctionException, ClassCastException, IOException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    GenericFunctionTableRow genericFunctionTableRow = (GenericFunctionTableRow) functionTableRow;

    String jarUrlString = genericFunctionTableRow.getJarUrl();
    String className = genericFunctionTableRow.getClassName();

    Class<?> functionClass;
    if (jarUrlString != null && jarUrlString.length() > 0) {
      // load class in custom jar using reflection
      LOGGER.info("loading jar at '{}'", jarUrlString);
      URL jarUrl = new URL(jarUrlString);
      URLClassLoader loader = new URLClassLoader(new URL[]{ jarUrl });
      functionClass = Class.forName(className, true, loader);
    } else {
      // load class in the anomaly jar by simple name
      LOGGER.info("loading builtin function '{}'", className);
      functionClass = Class.forName(BUILTIN_FUNCTION_QUALIFIED_NAME_PREFIX + className);
    }

    AnomalyDetectionFunction function = (AnomalyDetectionFunction) functionClass.newInstance();

    FunctionProperties functionProperties = null;

    // load function properties
    if (genericFunctionTableRow.getFunctionProperties() != null) {
      functionProperties = new FunctionProperties();
      functionProperties.load(new StringReader(genericFunctionTableRow.getFunctionProperties()));
    }

    function.init(starTreeConfig, functionProperties);
    return function;
  }

}
