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
package org.apache.pinot.spi.config.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class UserConfig extends BaseJsonConfig {
    public static final String USERNAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String COMPONET_KEY = "component";
    public static final String ROLE_KEY = "role";
    public static final String AUTH_TOKEN_KEY = "authToken";
    public static final String TABLES_KEY = "tables";
    public static final String EXCLUDE_TABLES_KEY = "excludeTables";
    public static final String PERMISSIONS_KEY = "permissions";

    @JsonPropertyDescription("The name of User")
    private String _username;

    @JsonPropertyDescription("The password of User")
    private String _password;

    @JsonPropertyDescription("The name of Component")
    private ComponentType _componentType;

    @JsonPropertyDescription("The role of user")
    private RoleType _roleType;

    @JsonPropertyDescription("The tables owned of User")
    private List<String> _tables;

    @JsonPropertyDescription("The tables excluded for User")
    private List<String> _excludeTables;

    @JsonPropertyDescription("The table permission of User")
    private List<AccessType> _permissions;

    @JsonCreator
    public UserConfig(@JsonProperty(value = USERNAME_KEY, required = true) String username,
        @JsonProperty(value = PASSWORD_KEY, required = true) String password,
        @JsonProperty(value = COMPONET_KEY, required = true) String component,
        @JsonProperty(value = ROLE_KEY, required = true) String role,
        @JsonProperty(value = TABLES_KEY) @Nullable List<String> tableList,
        @JsonProperty(value = EXCLUDE_TABLES_KEY) @Nullable List<String> excludeTableList,
        @JsonProperty(value = PERMISSIONS_KEY) @Nullable List<AccessType> permissionList
    ) {
        Preconditions.checkArgument(username != null, "'username' must be configured");
        Preconditions.checkArgument(password != null, "'password' must be configured");

        // NOTE: Handle lower case table type and raw table name for backward-compatibility
        _username = username;
        _password = password;
        _componentType = ComponentType.valueOf(component.toUpperCase());
        _roleType = RoleType.valueOf(role.toUpperCase());
        _tables = tableList;
        _excludeTables = excludeTableList;
        _permissions = permissionList;
    }

    @JsonProperty(USERNAME_KEY)
    public String getUserName() {
        return _username;
    }

    public String getUsernameWithComponent() {
        return getUserName() + "_" + getComponentType().toString();
    }

    public boolean isExist(String username, ComponentType component) {
        return _username.equals(username) && _componentType.equals(component);
    }

    @JsonProperty(PASSWORD_KEY)
    public String getPassword() {
        return _password;
    }

    @JsonProperty(TABLES_KEY)
    public List<String> getTables() {
        return _tables;
    }

    @JsonProperty(EXCLUDE_TABLES_KEY)
    public List<String> getExcludeTables() {
        return _excludeTables;
    }

    @JsonProperty(PERMISSIONS_KEY)
    public List<AccessType> getPermissios() {
        return _permissions;
    }

    @JsonProperty(COMPONET_KEY)
    public ComponentType getComponentType() {
        return _componentType;
    }

    @JsonProperty(ROLE_KEY)
    public RoleType getRoleType() {
        return _roleType;
    }

    public void setRole(String roleTypeStr) {
        _roleType = RoleType.valueOf(roleTypeStr);
    }

    public void setPassword(String password) {
        _password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserConfig that = (UserConfig) o;
        return _username.equals(that._username) && _componentType == that._componentType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), _username, _componentType);
    }
}
