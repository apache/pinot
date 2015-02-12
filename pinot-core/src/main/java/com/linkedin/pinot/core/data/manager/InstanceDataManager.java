package com.linkedin.pinot.core.data.manager;

import com.linkedin.pinot.common.data.DataManager;


public interface InstanceDataManager extends DataManager {
  ResourceDataManager getResourceDataManager(String resourceName);
}
