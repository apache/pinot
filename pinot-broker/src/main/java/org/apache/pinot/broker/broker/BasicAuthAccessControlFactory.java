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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthPrincipalUtils;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.TableRowColAccessResult;
import org.apache.pinot.spi.auth.TableRowColAccessResultImpl;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Basic Authentication based on http headers. Configured via the "pinot.broker.access.control" family of properties.
///
/// ```
/// Example:
/// pinot.broker.access.control.principals=admin123,user456
/// pinot.broker.access.control.principals.admin123.password=verysecret
/// pinot.broker.access.control.principals.user456.password=kindasecret
/// pinot.broker.access.control.principals.user456.tables=stuff,lessImportantStuff
/// pinot.broker.access.control.principals.user456.permissions=read,delete
/// ```
///
/// Principals query the tables they have access to. Deleting rows with a SQL `DELETE` also requires the `delete`
/// permission, granted explicitly: unlike on the controller, where a principal without permissions has them all, a
/// principal without permissions (or with `*`) does not delete rows, since the broker did not check the permissions
/// of its principals before.
public class BasicAuthAccessControlFactory extends AccessControlFactory {
  private static final String PREFIX = "principals";
  private static final String RLS_SUFFIX = ".rls";

  private AccessControl _accessControl;

  public BasicAuthAccessControlFactory() {
    // left blank
  }

  @Override
  public void init(PinotConfiguration configuration) {
    // LOOKUP may match a raw or physical grant. Keep the filter keys for both names even if only one name was
    // listed in the principal's table grants. Ordinary queries continue using the principal's parsed filters.
    Map<String, Boolean> lookupRowFilterKeys = configuration.getKeys().stream()
        .filter(key -> key.startsWith(PREFIX + ".") && key.endsWith(RLS_SUFFIX))
        .filter(key -> {
          String filters = configuration.getProperty(key);
          return filters != null && Arrays.stream(filters.split(",")).anyMatch(StringUtils::isNotBlank);
        })
        .collect(Collectors.toMap(key -> key, key -> Boolean.TRUE));
    _accessControl = new BasicAuthAccessControl(BasicAuthPrincipalUtils.extractBasicAuthPrincipals(configuration,
        PREFIX), new PinotConfiguration(lookupRowFilterKeys, Map.of()));
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /// Access Control using header-based basic http authentication
  private static class BasicAuthAccessControl implements AccessControl {
    private final Map<String, BasicAuthPrincipal> _token2principal;
    private final PinotConfiguration _lookupRowFilterKeys;

    public BasicAuthAccessControl(Collection<BasicAuthPrincipal> principals, PinotConfiguration lookupRowFilterKeys) {
      _token2principal = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
      _lookupRowFilterKeys = lookupRowFilterKeys;
    }

    @Override
    public AuthorizationResult authorize(RequesterIdentity requesterIdentity) {
      return authorize(requesterIdentity, (BrokerRequest) null);
    }

    @Override
    public AuthorizationResult authorize(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
      Optional<BasicAuthPrincipal> principalOpt = getPrincipalOpt(requesterIdentity);

      if (!principalOpt.isPresent()) {
        throw new NotAuthorizedException("Basic");
      }

      BasicAuthPrincipal principal = principalOpt.get();
      if (brokerRequest == null || !brokerRequest.isSetQuerySource() || !brokerRequest.getQuerySource()
          .isSetTableName()) {
        // no table restrictions? accept
        return TableAuthorizationResult.success();
      }

      Set<String> failedTables = new HashSet<>();

      if (!principal.hasTable(brokerRequest.getQuerySource().getTableName())) {
        failedTables.add(brokerRequest.getQuerySource().getTableName());
      }
      if (failedTables.isEmpty()) {
        return TableAuthorizationResult.success();
      }
      return new TableAuthorizationResult(failedTables);
    }

    @Override
    public TableAuthorizationResult authorize(RequesterIdentity requesterIdentity, Set<String> tables) {
      return authorizeTables(requesterIdentity, tables, BasicAuthPrincipal::hasTable);
    }

    @Override
    public TableAuthorizationResult authorizeLookupTables(RequesterIdentity requesterIdentity,
        Set<String> offlineTableNames) {
      return authorizeTables(requesterIdentity, offlineTableNames, BasicAuthPrincipal::hasLookupTable);
    }

    private TableAuthorizationResult authorizeTables(RequesterIdentity requesterIdentity, Set<String> tables,
        BiPredicate<BasicAuthPrincipal, String> hasTableAccess) {
      Optional<BasicAuthPrincipal> principalOpt = getPrincipalOpt(requesterIdentity);

      if (!principalOpt.isPresent()) {
        throw new NotAuthorizedException("Basic");
      }

      if (tables == null || tables.isEmpty()) {
        return TableAuthorizationResult.success();
      }
      BasicAuthPrincipal principal = principalOpt.get();
      Set<String> failedTables = new HashSet<>();
      for (String table : tables) {
        if (!hasTableAccess.test(principal, table)) {
          failedTables.add(table);
        }
      }
      if (failedTables.isEmpty()) {
        return TableAuthorizationResult.success();
      }
      return new TableAuthorizationResult(failedTables);
    }

    @Override
    public AuthorizationResult authorizeDeleteRows(RequesterIdentity requesterIdentity,
        @Nullable HttpHeaders httpHeaders, String tableName) {
      BasicAuthPrincipal principal = getPrincipalOpt(requesterIdentity).orElse(null);
      // The table is matched by the name as given, as for queries: `tables` lists the name as given, and `hasTable`
      // checks `excludeTables`, which lists raw names, on the raw name too, so that a type suffix cannot bypass an
      // excluded table
      return BasicAuthPrincipalUtils.authorizeDeleteRows(principal, tableName,
          principal != null && principal.hasTable(tableName));
    }

    @Override
    public boolean hasLookupTableRowFilters(RequesterIdentity requesterIdentity, String offlineTableName) {
      BasicAuthPrincipal principal = getPrincipalOpt(requesterIdentity)
          .orElseThrow(() -> new IllegalStateException("Principal is not authorized"));
      String principalPrefix = PREFIX + "." + principal.getName() + ".";
      String rawTableName = TableNameBuilder.extractRawTableName(offlineTableName);
      return _lookupRowFilterKeys.containsKey(principalPrefix + offlineTableName + RLS_SUFFIX)
          || _lookupRowFilterKeys.containsKey(principalPrefix + rawTableName + RLS_SUFFIX);
    }

    @Override
    public TableRowColAccessResult getRowColFilters(RequesterIdentity requesterIdentity, String table) {
      Optional<BasicAuthPrincipal> principalOpt = getPrincipalOpt(requesterIdentity);

      Preconditions.checkState(principalOpt.isPresent(), "Principal is not authorized");
      Preconditions.checkState(table != null, "Table cannot be null");

      TableRowColAccessResult tableRowColAccessResult = new TableRowColAccessResultImpl();
      BasicAuthPrincipal principal = principalOpt.get();

      //precondition: The principal should have the table.
      Preconditions.checkArgument(principal.hasTable(table),
          "Principal: " + principal.getName() + " does not have access to table: " + table);

      Optional<List<String>> rlsFiltersMaybe = principal.getRLSFilters(table);
      rlsFiltersMaybe.ifPresent(tableRowColAccessResult::setRLSFilters);

      return tableRowColAccessResult;
    }

    private Optional<BasicAuthPrincipal> getPrincipalOpt(RequesterIdentity requesterIdentity) {
      Collection<String> tokens = extractAuthorizationTokens(requesterIdentity);
      if (tokens.isEmpty()) {
        return Optional.empty();
      }
      return tokens.stream().map(BasicAuthTokenUtils::normalizeBase64Token)
          .map(_token2principal::get).filter(Objects::nonNull)
          .findFirst();
    }
  }
}
