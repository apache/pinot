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
package org.apache.pinot.controller.api.access;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.env.PinotConfiguration;

import javax.ws.rs.core.HttpHeaders;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Basic Authentication based on http headers. Configured via the "controller.admin.access.control" family of properties.
 *
 * <pre>
 *     Example:
 *     controller.admin.access.control.principals=admin123,user456
 *     controller.admin.access.control.principals.admin123.password=verysecret
 *     controller.admin.access.control.principals.user456.password=kindasecret
 *     controller.admin.access.control.principals.user456.tables=stuff,lessImportantStuff
 *     controller.admin.access.control.principals.user456.permissions=read,update
 * </pre>
 */
public class BasicAuthAccessControlFactory implements AccessControlFactory {
  private static final String PREFIX = "controller.admin.access.control.principals";
  private static final String PASSWORD = "password";
  private static final String PERMISSIONS = "permissions";
  private static final String TABLES = "tables";
  private static final String ALL = "*";

  private static final String HEADER_AUTHORIZATION = "Authorization";

  private AccessControl _accessControl;

  public void init(PinotConfiguration configuration) {
    String principalNames = configuration.getProperty(PREFIX);
    Preconditions.checkArgument(StringUtils.isNotBlank(principalNames), "must provide principals");

    List<BasicAuthPrincipal> principals = Arrays.stream(principalNames.split(",")).map(rawName -> {
      String name = rawName.trim();
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "%s is not a valid name", name);

      String password = configuration.getProperty(String.format("%s.%s.%s", PREFIX, name, PASSWORD));
      Preconditions.checkArgument(StringUtils.isNotBlank(password), "must provide a password for %s", name);

      Set<String> tables = new HashSet<>();
      String tableNames = configuration.getProperty(String.format("%s.%s.%s", PREFIX, name, TABLES));
      if (StringUtils.isNotBlank(tableNames) && !ALL.equals(tableNames)) {
        tables = Arrays.stream(tableNames.split(",")).map(String::trim).collect(Collectors.toSet());
      }

      Set<AccessType> permissions = new HashSet<>();
      String permissionNames = configuration.getProperty(String.format("%s.%s.%s", PREFIX, name, PERMISSIONS));
      if (StringUtils.isNotBlank(permissionNames) && !ALL.equals(tableNames)) {
        permissions = Arrays.stream(permissionNames.split(",")).map(String::trim).map(String::toUpperCase)
                .map(AccessType::valueOf).collect(Collectors.toSet());
      }

      return new BasicAuthPrincipal(name, toToken(name, password), tables, permissions);
    }).collect(Collectors.toList());

    _accessControl = new BasicAuthAccessControl(principals);
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using header-based basic http authentication
   */
  private static class BasicAuthAccessControl implements AccessControl {
    private final Map<String, BasicAuthPrincipal> _principals;

    public BasicAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
      this._principals = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
    }

    @Override
    public boolean hasDataAccess(HttpHeaders httpHeaders, String tableName) {
      return getPrincipal(httpHeaders).filter(p -> p.hasTable(tableName)).isPresent();
    }

    @Override
    public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      return getPrincipal(httpHeaders).filter(p -> p.hasTable(tableName) && p.hasPermission(accessType)).isPresent();
    }

    @Override
    public boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      return getPrincipal(httpHeaders).isPresent();
    }

    private Optional<BasicAuthPrincipal> getPrincipal(HttpHeaders headers) {
      return headers.getRequestHeader(HEADER_AUTHORIZATION).stream().map(BasicAuthAccessControlFactory::normalizeToken)
              .map(_principals::get).filter(Objects::nonNull).findFirst();
    }
  }

  /**
   * Container object for basic auth principal
   */
  private static class BasicAuthPrincipal {
    private final String _name;
    private final String _token;
    private final Set<String> _tables;
    private final Set<AccessType> _permissions;

    public BasicAuthPrincipal(String name, String token, Set<String> tables, Set<AccessType> permissions) {
      this._name = name;
      this._token = token;
      this._tables = tables;
      this._permissions = permissions;
    }

    public String getName() {
      return _name;
    }

    public String getToken() {
      return _token;
    }

    public boolean hasTable(String tableName) {
      return _tables.isEmpty() || _tables.contains(tableName);
    }

    public boolean hasPermission(AccessType accessType) {
      return _permissions.isEmpty() || _permissions.contains(accessType);
    }
  }

  private static String toToken(String name, String password) {
    String identifier = String.format("%s:%s", name, password);
    return normalizeToken(String.format("Basic %s", Base64.getEncoder().encodeToString(identifier.getBytes())));
  }

  private static String normalizeToken(String token) {
    if (token == null) {
      return null;
    }
    return token.trim().replace("=", "");
  }
}
