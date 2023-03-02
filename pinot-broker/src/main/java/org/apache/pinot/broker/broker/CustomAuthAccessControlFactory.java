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
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.core.auth.CustomAuthUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


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
public class CustomAuthAccessControlFactory extends AccessControlFactory {
    private static final String PREFIX = "principals";

    private static final String BROKER_PREFIX = "pinot.broker.access.control.principals";

    private static final String HEADER_AUTHORIZATION = "authorization";

    private AccessControl _accessControl;

    public CustomAuthAccessControlFactory() {
        // left blank
    }

    @Override
    public void init(PinotConfiguration configuration) {
        String listOfPrincipals = System.getenv("AUTH_PRINCIPALS");
        configuration.setProperty(BROKER_PREFIX, System.getenv("AUTH_PRINCIPALS"));
        for (String x : listOfPrincipals.split(",")) {
            configuration.setProperty(BROKER_PREFIX + "." + x + ".password",
                    System.getenv("AUTH_" + x.toUpperCase(Locale.ROOT) + "_PASSWORD"));
        }
        System.out.println(configuration.toString());
        _accessControl = new CustomAuthAccessControl(
                CustomAuthUtils.extractBasicAuthPrincipals(configuration, BROKER_PREFIX));
    }

    @Override
    public AccessControl create() {
        return _accessControl;
    }

    /**
     * Access Control using header-based basic http authentication
     */
    private static class CustomAuthAccessControl implements AccessControl {
        private final Map<String, BasicAuthPrincipal> _token2principal;

        public CustomAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
            _token2principal = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
        }

        @Override
        public boolean hasAccess(RequesterIdentity requesterIdentity) {
            return hasAccess(requesterIdentity, null);
        }

        @Override
        public boolean hasAccess(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
            Preconditions.checkArgument(requesterIdentity instanceof HttpRequesterIdentity,
                    "HttpRequesterIdentity required");
            HttpRequesterIdentity identity = (HttpRequesterIdentity) requesterIdentity;

            Collection<String> tokens = identity.getHttpHeaders().get(HEADER_AUTHORIZATION);
            Optional<BasicAuthPrincipal> principalOpt =
                    tokens.stream().map(BasicAuthUtils::normalizeBase64Token).map(_token2principal::get)
                            .filter(Objects::nonNull)
                            .findFirst();

            if (!principalOpt.isPresent()) {
                // no matching token? reject
                return false;
            }

            BasicAuthPrincipal principal = principalOpt.get();
            if (brokerRequest == null || !brokerRequest.isSetQuerySource() || !brokerRequest.getQuerySource()
                    .isSetTableName()) {
                // no table restrictions? accept
                return true;
            }

            return principal.hasTable(brokerRequest.getQuerySource().getTableName());
        }
    }
}
