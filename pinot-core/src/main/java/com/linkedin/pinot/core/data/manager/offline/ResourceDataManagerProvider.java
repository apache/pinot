/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.manager.offline;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeResourceDataManager;


/**
 * Provide static function to get PartitionDataManager implementation.
 * @author xiafu
 *
 */
public class ResourceDataManagerProvider {

  private static Map<String, Class<? extends ResourceDataManager>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends ResourceDataManager>>();

  static {
    keyToFunction.put("offline", OfflineResourceDataManager.class);
    keyToFunction.put("realtime", RealtimeResourceDataManager.class);
  }

  public static ResourceDataManager getResourceDataManager(ResourceDataManagerConfig resourceDataManagerConfig) {
    try {
      Class<? extends ResourceDataManager> cls =
          keyToFunction.get(resourceDataManagerConfig.getResourceDataManagerType().toLowerCase());
      if (cls != null) {
        ResourceDataManager resourceDataManager = (ResourceDataManager) cls.newInstance();
        resourceDataManager.init(resourceDataManagerConfig);
        return resourceDataManager;
      } else {
        throw new UnsupportedOperationException("No ResourceDataManager type found.");
      }
    } catch (Exception ex) {
      throw new RuntimeException("Not support ResourceDataManager type with - "
          + resourceDataManagerConfig.getResourceDataManagerType());
    }
  }

}
