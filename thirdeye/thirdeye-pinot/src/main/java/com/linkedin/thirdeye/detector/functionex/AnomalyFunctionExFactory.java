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

  private AnomalyFunctionExContext populateFromDataSources(AnomalyFunctionExContext context) {
    Map<String, AnomalyFunctionExDataSource> ds = new HashMap<>();
    for(HashMap.Entry<String, AnomalyFunctionExDataSource> e : dataSources.entrySet()) {
      ds.put(e.getKey(), e.getValue());
    }
    context.setDataSources(Collections.unmodifiableMap(ds));
    return context;
  }

  public AnomalyFunctionEx fromContext(AnomalyFunctionExContext context) throws Exception {
    populateFromDataSources(context);

    // instantiate class
    Class<?> clazz = Class.forName(context.getClassName());
    AnomalyFunctionEx func = (AnomalyFunctionEx) clazz.newInstance();

    // TODO: reconsider setter vs constructor injection
    func.setContext(context);

    return func;
  }

  public void addDataSource(String identifier, AnomalyFunctionExDataSource dataSource) {
    if(dataSources.containsKey(identifier))
      throw new IllegalArgumentException(String.format("DataSource with identifier '%s' already registered", identifier));
    dataSources.put(identifier, dataSource);
  }

}
