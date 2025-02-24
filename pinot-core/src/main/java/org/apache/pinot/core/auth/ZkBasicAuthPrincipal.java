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
package org.apache.pinot.core.auth;

import java.util.Set;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;

/**
 * Container object for basic auth principal
 */
public class ZkBasicAuthPrincipal extends BasicAuthPrincipal {
    private final String _password;
    private final String _component;
    private final String _role;

    public ZkBasicAuthPrincipal(String name, String token, String password, String component, String role,
        Set<String> tables, Set<String> excludeTables, Set<String> permissions) {
        super(name, token, tables, excludeTables, permissions);
        _component = component;
        _role = role;
        _password = password;
    }

    public String getRole() {
        return _role;
    }

    public String getComponent() {
        return _component;
    }

    public String getPassword() {
        return _password;
    }

    public boolean hasPermission(RoleType role, ComponentType component) {
        return RoleType.valueOf(_role).equals(role) && ComponentType.valueOf(_component).equals(component);
    }
}
