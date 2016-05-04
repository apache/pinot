package com.linkedin.thirdeye.dashboard.configs;

import java.util.List;

public abstract class AbstractConfigDAO<T extends AbstractConfig> {

  public abstract List<T> findAll(String collectionName);
  
  public abstract T findById(String collectionName, String id);
  
  public abstract boolean create(String collectionName, String id, T config) throws Exception;
  
  public abstract boolean update(String collectionName, String id, T config) throws Exception;
  
}
