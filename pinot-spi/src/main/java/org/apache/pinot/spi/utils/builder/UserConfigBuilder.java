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
package org.apache.pinot.spi.utils.builder;

import java.util.List;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;

public class UserConfigBuilder {

    private ComponentType _componentType;
    private String _username;
    private String _password;
    private RoleType _roleType;
    private List<String> _tableList;
    private List<String> _excludeTableList;
    private List<AccessType> _permissionList;

    public UserConfigBuilder setComponentType(ComponentType componentType) {
        _componentType = componentType;
        return this;
    }

    public UserConfigBuilder setUsername(String username) {
        _username = username;
        return this;
    }

    public UserConfigBuilder setPassword(String password) {
        _password = password;
        return this;
    }

    public UserConfigBuilder setRoleType(RoleType roleType) {
        _roleType = roleType;
        return this;
    }

    public UserConfigBuilder setTableList(List<String> tableList) {
        _tableList = tableList;
        return this;
    }

    public UserConfigBuilder setExcludeTableList(List<String> excludeTableList) {
        _excludeTableList = excludeTableList;
        return this;
    }

    public UserConfigBuilder setPermissionList(List<AccessType> permissionList) {
        _permissionList = permissionList;
        return this;
    }

    public UserConfig build() {
        return new UserConfig(_username, _password, _componentType.toString(), _roleType.toString(), _tableList,
            _excludeTableList, _permissionList);
    }
}
