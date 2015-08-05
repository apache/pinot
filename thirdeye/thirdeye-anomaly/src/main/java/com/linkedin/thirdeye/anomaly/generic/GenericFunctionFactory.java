package com.linkedin.thirdeye.anomaly.generic;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;

import org.omg.CORBA.portable.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionFactory;
import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 *
 */
public class GenericFunctionFactory extends AnomalyDetectionFunctionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericFunctionFactory.class);

  @Override
  public Class<? extends FunctionTableRow> getFunctionRowClass() {
    return GenericFunctionTableRow.class;
  }

  @Override
  public AnomalyDetectionFunction getFunction(StarTreeConfig starTreeConfig, AnomalyDatabaseConfig dbconfig,
      FunctionTableRow functionTableRow) throws IllegalFunctionException, ClassCastException, IOException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    GenericFunctionTableRow genericFunctionTableRow = (GenericFunctionTableRow) functionTableRow;

    String jarUrlString = genericFunctionTableRow.getJarUrl();
    String className = genericFunctionTableRow.getClassName();

    LOGGER.info("loading jar at '{}'", jarUrlString);

    URL jarUrl = new URL(jarUrlString);
    URLClassLoader loader = new URLClassLoader(new URL[]{ jarUrl });
    Class<?> functionClass = Class.forName(className, true, loader);

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
