package com.linkedin.thirdeye.detector.functionex;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyFunctionExFactory {
  static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionExFactory.class);

  private Map<String, AnomalyFunctionExDataSource> dataSources = new HashMap<>();

  public AnomalyFunctionEx fromSpec(AnomalyFunctionExDTO spec) throws Exception {

    // populate context
    AnomalyFunctionExContext context = new AnomalyFunctionExContext();

    context.setName(spec.getName());
    context.setConfig(Collections.unmodifiableMap(spec.getConfig()));

    Map<String, AnomalyFunctionExDataSource> ds = new HashMap<>();
    for(HashMap.Entry<String, AnomalyFunctionExDataSource> e : dataSources.entrySet()) {
      ds.put(e.getKey(), e.getValue());
    }
    context.setDataSources(Collections.unmodifiableMap(ds));

    // instantiate class
    String className = spec.getClassName();
    Class<?> clazz = Class.forName(className);
    AnomalyFunctionEx func = (AnomalyFunctionEx) clazz.newInstance();

    // NOTE: setter injection for client convenience
    func.setContext(context);

    return func;
  }

  public void addDataSource(String identifier, AnomalyFunctionExDataSource dataSource) {
    if(dataSources.containsKey(identifier))
      throw new IllegalArgumentException(String.format("DataSource with identifier '%s' already registered", identifier));
    dataSources.put(identifier, dataSource);
  }

}
