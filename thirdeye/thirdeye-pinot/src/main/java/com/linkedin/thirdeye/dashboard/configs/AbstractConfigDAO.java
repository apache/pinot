package com.linkedin.thirdeye.dashboard.configs;

import java.util.List;

public abstract class AbstractConfigDAO<T extends AbstractConfig> {

  public abstract List<T> findAll();
  
  public abstract T findById(String id);
  
  public abstract boolean create(String id, T config) throws Exception;
  
  public abstract boolean update(String id, T config) throws Exception;
  
}
