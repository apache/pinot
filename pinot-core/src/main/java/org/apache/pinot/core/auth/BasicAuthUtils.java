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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Utility for configuring basic auth and parsing related http tokens
 */
public final class BasicAuthUtils {
  private static final String PASSWORD = "password";
  private static final String PERMISSIONS = "permissions";
  private static final String TABLES = "tables";
  private static final String ALL = "*";

  private BasicAuthUtils() {
    // left blank
  }

  /**
   * Parse a pinot configuration namespace for access control settings, e.g. "controller.admin.access.control
   * .principals".
   *
   * <pre>
   *     Example:
   *     my.prefix.access.control.principals=admin123,user456
   *     my.prefix.access.control.principals.admin123.password=verysecret
   *     my.prefix.access.control.principals.user456.password=kindasecret
   *     my.prefix.access.control.principals.user456.tables=stuff,lessImportantStuff
   *     my.prefix.access.control.principals.user456.permissions=read,update
   * </pre>
   *
   * @param configuration pinot configuration
   * @param prefix configuration namespace
   * @return list of BasicAuthPrincipals
   */
  public static List<BasicAuthPrincipal> extractBasicAuthPrincipals(PinotConfiguration configuration, String prefix) {
    String principalNames = configuration.getProperty(prefix);
    Preconditions.checkArgument(StringUtils.isNotBlank(principalNames), "must provide principals");

    return Arrays.stream(principalNames.split(",")).map(rawName -> {
      String name = rawName.trim();
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "%s is not a valid name", name);

      String password = configuration.getProperty(String.format("%s.%s.%s", prefix, name, PASSWORD));
      Preconditions.checkArgument(StringUtils.isNotBlank(password), "must provide a password for %s", name);

      Set<String> tables = extractSet(configuration, String.format("%s.%s.%s", prefix, name, TABLES));
      Set<String> permissions = extractSet(configuration, String.format("%s.%s.%s", prefix, name, PERMISSIONS));

      return new BasicAuthPrincipal(name, org.apache.pinot.common.auth.BasicAuthUtils.toBasicAuthToken(name, password),
          tables, permissions);
    }).collect(Collectors.toList());
  }

  public static List<ZkBasicAuthPrincipal> extractBasicAuthPrincipals(List<UserConfig> userConfigList) {
    return userConfigList.stream()
        .map(user -> {
          String name = user.getUserName().trim();
          Preconditions.checkArgument(StringUtils.isNotBlank(name), "%s is not a valid username", name);
          String password = user.getPassword().trim();
          Preconditions.checkArgument(StringUtils.isNotBlank(password), "must provide a password for %s", name);
          String component = user.getComponentType().toString();
          String role = user.getRoleType().toString();

          Set<String> tables = Optional.ofNullable(user.getTables())
              .orElseGet(() -> Collections.emptyList())
              .stream().collect(Collectors.toSet());
          Set<String> permissions = Optional.ofNullable(user.getPermissios())
              .orElseGet(() -> Collections.emptyList())
              .stream().map(x -> x.toString())
              .collect(Collectors.toSet());
          return new ZkBasicAuthPrincipal(name,
              org.apache.pinot.common.auth.BasicAuthUtils.toBasicAuthToken(name, password), password,
              component, role, tables, permissions);
        }).collect(Collectors.toList());
  }

  private static Set<String> extractSet(PinotConfiguration configuration, String key) {
    String input = configuration.getProperty(key);
    if (StringUtils.isNotBlank(input) && !ALL.equals(input)) {
      return Arrays.stream(input.split(",")).map(String::trim).collect(Collectors.toSet());
    }
    return Collections.emptySet();
  }
}
