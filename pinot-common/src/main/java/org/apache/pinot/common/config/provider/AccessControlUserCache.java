/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.config.provider;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code AccessControlUserCache} caches all the user configs within the cluster, and listens on ZK changes to keep
 * them in sync. It also maintains several component user config maps.
 */
public class AccessControlUserCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(AccessControlUserCache.class);
  private static final String USER_CONFIG_PARENT_PATH = "/CONFIGS/USER";
  private static final String USER_CONFIG_PATH_PREFIX = "/CONFIGS/USER/";
  private static final int USER_PASSWORD_EXPIRE_TIME = 24 * 60 * 60;

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private final UserConfigChangeListener _userConfigChangeListener = new UserConfigChangeListener();

  private final Map<String, UserConfig> _userConfigMap = new ConcurrentHashMap<>();
  private final Map<String, UserConfig> _userControllerConfigMap = new ConcurrentHashMap<>();
  private final Map<String, UserConfig> _userBrokerConfigMap = new ConcurrentHashMap<>();
  private final Map<String, UserConfig> _userServerConfigMap = new ConcurrentHashMap<>();
  private Cache<String, String> _userPasswordAuthCache =
      CacheBuilder.newBuilder().expireAfterWrite(USER_PASSWORD_EXPIRE_TIME, TimeUnit.SECONDS).build();

  public AccessControlUserCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    _propertyStore.subscribeChildChanges(USER_CONFIG_PARENT_PATH, _userConfigChangeListener);

    List<String> users = _propertyStore.getChildNames(USER_CONFIG_PARENT_PATH, AccessOption.PERSISTENT);
    if (CollectionUtils.isNotEmpty(users)) {
      List<String> pathsToAdd = new ArrayList<>(users.size());
      for (String user : users) {
        pathsToAdd.add(USER_CONFIG_PATH_PREFIX + user);
      }
      addUserConfigs(pathsToAdd);
    }
  }

  public Cache<String, String> getUserPasswordAuthCache() {
    return _userPasswordAuthCache;
  }

  @Nullable
  public List<UserConfig> getAllUserConfig() {
    return _userConfigMap.values().stream().collect(Collectors.toList());
  }

  public List<UserConfig> getAllControllerUserConfig() {
    return _userControllerConfigMap.values().stream().collect(Collectors.toList());
  }

  public List<UserConfig> getAllBrokerUserConfig() {
    return _userBrokerConfigMap.values().stream().collect(Collectors.toList());
  }

  public List<UserConfig> getAllServerUserConfig() {
    return _userServerConfigMap.values().stream().collect(Collectors.toList());
  }

  @Nullable
  public List<String> getAllUserName() {
    return _userConfigMap.keySet().stream().collect(Collectors.toList());
  }

  private void addUserConfigs(List<String> allUserZKStorePath) {
    for (String userStoreZKPath : allUserZKStorePath) {
      _propertyStore.subscribeDataChanges(userStoreZKPath, _userConfigChangeListener);
    }
    List<ZNRecord> allUserRecords = _propertyStore.get(allUserZKStorePath, null, AccessOption.PERSISTENT, false);
    for (ZNRecord userRecord : allUserRecords) {
      if (userRecord != null) {
        try {
          UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(userRecord);
          String username = userConfig.getUsernameWithComponent();

          if (userConfig.getComponentType().equals(ComponentType.CONTROLLER)) {
            _userControllerConfigMap.put(username, userConfig);
          } else if (userConfig.getComponentType().equals(ComponentType.BROKER)) {
            _userBrokerConfigMap.put(username, userConfig);
          } else if (userConfig.getComponentType().equals(ComponentType.SERVER)) {
            _userServerConfigMap.put(username, userConfig);
          }
        } catch (Exception e) {
          LOGGER.error("Caught exception while adding user config for ZNRecord:{}", userRecord.getId(), e);
        }
      }
    }
  }

  private void removeUserConfig(String userStoreZKPath) {
    _propertyStore.unsubscribeDataChanges(userStoreZKPath, _userConfigChangeListener);
    String usernameWithComponentType = userStoreZKPath.substring(USER_CONFIG_PATH_PREFIX.length());
    if (usernameWithComponentType.endsWith("BROKER")) {
      _userBrokerConfigMap.remove(usernameWithComponentType);
    } else if (usernameWithComponentType.endsWith("CONTROLLER")) {
      _userControllerConfigMap.remove(usernameWithComponentType);
    } else if (usernameWithComponentType.endsWith("SERVER")) {
      _userServerConfigMap.remove(usernameWithComponentType);
    }
  }

  private class UserConfigChangeListener implements IZkChildListener, IZkDataListener {
    @Override
    public void handleChildChange(String userStoreZKPath, List<String> allUserList)
        throws Exception {
      if (CollectionUtils.isEmpty(allUserList)) {
        return;
      }

      List<String> newUserPathToAdd = new ArrayList<>();
      for (String user : allUserList) {
        if (!_userControllerConfigMap.containsKey(user) && !_userBrokerConfigMap.containsKey(user)
            && !_userServerConfigMap.containsKey(user)) {
          newUserPathToAdd.add(USER_CONFIG_PATH_PREFIX + user);
        }
      }
      if (!newUserPathToAdd.isEmpty()) {
        addUserConfigs(newUserPathToAdd);
      }
    }

    @Override
    public void handleDataChange(String userStoreZKPath, Object changedUserConfig)
        throws Exception {
      if (changedUserConfig != null) {
        ZNRecord userRecord = (ZNRecord) changedUserConfig;
        try {
          UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(userRecord);
          String usernameWithComponentType = userConfig.getUsernameWithComponent();
          if (userConfig.getComponentType().equals(ComponentType.CONTROLLER)) {
            _userControllerConfigMap.put(usernameWithComponentType, userConfig);
          } else if (userConfig.getComponentType().equals(ComponentType.BROKER)) {
            _userBrokerConfigMap.put(usernameWithComponentType, userConfig);
          } else if (userConfig.getComponentType().equals(ComponentType.SERVER)) {
            _userServerConfigMap.put(usernameWithComponentType, userConfig);
          }
        } catch (Exception e) {
          LOGGER.error("caught exception while refreshing table config for ZNRecord: {}", userRecord.getId(), e);
        }
      }
    }

    @Override
    public void handleDataDeleted(String path)
        throws Exception {
      String username = path.substring(path.lastIndexOf('/') + 1);
      removeUserConfig(USER_CONFIG_PATH_PREFIX + username);
    }
  }
}
