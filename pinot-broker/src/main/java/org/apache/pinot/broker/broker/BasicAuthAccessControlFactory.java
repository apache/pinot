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
package org.apache.pinot.broker.broker;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.env.PinotConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Basic Authentication based on http headers. Configured via the "pinot.broker.access.control" family of properties.
 *
 * <pre>
 *     Example:
 *     pinot.broker.access.control.principals=admin123,user456
 *     pinot.broker.access.control.principals.admin123.password=verysecret
 *     pinot.broker.access.control.principals.user456.password=kindasecret
 *     pinot.broker.access.control.principals.user456.tables=stuff,lessImportantStuff
 * </pre>
 */
public class BasicAuthAccessControlFactory extends AccessControlFactory {
  private static final String PRINCIPALS = "principals";
  private static final String PASSWORD = "password";
  private static final String TABLES = "tables";
  private static final String TABLES_ALL = "*";

  private static final String HEADER_AUTHORIZATION = "authorization";

  private AccessControl _accessControl;

  public BasicAuthAccessControlFactory() {
    // left blank
  }

  public void init(PinotConfiguration configuration) {
    String principalNames = configuration.getProperty(PRINCIPALS);
    Preconditions.checkArgument(StringUtils.isNotBlank(principalNames), "must provide principals");

    List<BasicAuthPrincipal> principals = Arrays.stream(principalNames.split(",")).map(rawName -> {
      String name = rawName.trim();
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "%s is not a valid name", name);

      String password = configuration.getProperty(String.format("%s.%s.%s", PRINCIPALS, name, PASSWORD));
      Preconditions.checkArgument(StringUtils.isNotBlank(password), "must provide a password for %s", name);

      Set<String> tables = new HashSet<>();
      String tableNames = configuration.getProperty(String.format("%s.%s.%s", PRINCIPALS, name, TABLES));
      if (StringUtils.isNotBlank(tableNames) && !TABLES_ALL.equals(tableNames)) {
        tables.addAll(Arrays.asList(tableNames.split(",")));
      }

      return new BasicAuthPrincipal(name, toToken(name, password), tables);
    }).collect(Collectors.toList());

    _accessControl = new BasicAuthAccessControl(principals);
  }

  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using header-based basic http authentication
   */
  private static class BasicAuthAccessControl implements AccessControl {
    private final Map<String, BasicAuthPrincipal> _principals;

    public BasicAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
      _principals = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
    }

    @Override
    public boolean hasAccess(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
      Preconditions.checkArgument(requesterIdentity instanceof HttpRequesterIdentity, "HttpRequesterIdentity required");
      HttpRequesterIdentity identity = (HttpRequesterIdentity) requesterIdentity;

      Collection<String> tokens = identity.getHttpHeaders().get(HEADER_AUTHORIZATION);
      Optional<BasicAuthPrincipal> principalOpt =
          tokens.stream().map(BasicAuthAccessControlFactory::normalizeToken).map(_principals::get)
              .filter(Objects::nonNull).findFirst();

      if (!principalOpt.isPresent()) {
        // no matching token? reject
        return false;
      }

      BasicAuthPrincipal principal = principalOpt.get();
      if (principal.getTables().isEmpty() || !brokerRequest.isSetQuerySource() || !brokerRequest.getQuerySource()
          .isSetTableName()) {
        // no table restrictions? accept
        return true;
      }

      return principal.getTables().contains(brokerRequest.getQuerySource().getTableName());
    }
  }

  /**
   * Container object for basic auth principal
   */
  private static class BasicAuthPrincipal {
    private final String _name;
    private final String _token;
    private final Set<String> _tables;

    public BasicAuthPrincipal(String name, String token, Set<String> tables) {
      _name = name;
      _token = token;
      _tables = tables;
    }

    public String getName() {
      return _name;
    }

    public Set<String> getTables() {
      return _tables;
    }

    public String getToken() {
      return _token;
    }
  }

  private static String toToken(String name, String password) {
    String identifier = String.format("%s:%s", name, password);
    return normalizeToken(
        String.format("Basic %s", Base64.getEncoder().encodeToString(identifier.getBytes(StandardCharsets.UTF_8))));
  }

  /**
   * Implementations of base64 encoding vary and may generate different numbers of padding characters "=". We normalize
   * these by removing any padding.
   *
   * @param token raw token
   * @return normalized token
   */
  @Nullable
  private static String normalizeToken(String token) {
    if (token == null) {
      return null;
    }
    return StringUtils.remove(token.trim(), '=');
  }
}
