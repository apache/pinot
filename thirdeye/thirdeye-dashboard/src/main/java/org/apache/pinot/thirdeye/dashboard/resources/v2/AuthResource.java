/*
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

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import static org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter.AUTH_TOKEN_NAME;
import java.util.Optional;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter;
import org.apache.pinot.thirdeye.auth.ThirdEyeCredentials;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datalayer.dto.SessionDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.SessionBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import io.dropwizard.auth.Authenticator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/auth")
@Produces(MediaType.APPLICATION_JSON)
public class AuthResource {
  private static final Logger LOG = LoggerFactory.getLogger(AuthResource.class);
  private final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final int DEFAULT_VALID_DAYS_VALUE = 90;
  private final Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> authenticator;
  private final long cookieTTL;
  private final SessionManager sessionDAO;
  private final Random random;

  public AuthResource(Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> authenticator,
      long cookieTTL) {
    this.authenticator = authenticator;
    this.cookieTTL = cookieTTL;
    this.sessionDAO = DAO_REGISTRY.getSessionDAO();
    this.random = new Random();
    this.random.setSeed(System.currentTimeMillis());
  }

  /**
   * Create service token for a service.
   *
   * @param service the service
   * @param validDays the number of valid days
   * @return the token
   */
  @Path("/create-token")
  @POST
  public Response createServiceToken(@QueryParam("service") @NotNull String service, @QueryParam("validDays") Integer validDays){
    String serviceToken = generateSessionKey(service);
    SessionDTO sessionDTO = new SessionDTO();
    sessionDTO.setSessionKey(serviceToken);
    sessionDTO.setPrincipal(service);
    sessionDTO.setPrincipalType(SessionBean.PrincipalType.SERVICE);
    if (validDays == null){
      validDays = DEFAULT_VALID_DAYS_VALUE;
    }
    sessionDTO.setExpirationTime(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(validDays));

    this.sessionDAO.save(sessionDTO);
    return Response.ok(serviceToken).build();
  }

  @Path("/authenticate")
  @POST
  public Response authenticate(ThirdEyeCredentials credentials) {
    try {
      final Optional<ThirdEyePrincipal> optPrincipal = this.authenticator.authenticate(credentials);
      if (!optPrincipal.isPresent()) {
        return Response.status(Response.Status.UNAUTHORIZED).build();
      }

      final ThirdEyePrincipal principal = optPrincipal.get();

      String sessionKey = generateSessionKey(principal.getName());
      SessionDTO sessionDTO = new SessionDTO();
      sessionDTO.setSessionKey(sessionKey);
      sessionDTO.setPrincipalType(SessionBean.PrincipalType.USER);
      sessionDTO.setPrincipal(principal.getName());
      sessionDTO.setExpirationTime(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24));
      this.sessionDAO.save(sessionDTO);

      NewCookie cookie =
          new NewCookie(AUTH_TOKEN_NAME, sessionKey, "/", null, null, (int) (this.cookieTTL / 1000), false);

      return Response.ok(principal).cookie(cookie).build();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return Response.status(Response.Status.UNAUTHORIZED).build();
  }
  @Path("/logout")
  @GET
  public Response logout() {
    ThirdEyePrincipal principal = ThirdEyeAuthFilter.getCurrentPrincipal();

    if (principal != null) {
      // if not logged out already
      this.sessionDAO.deleteByPredicate(Predicate.EQ("sessionKey", principal.getSessionKey()));
    }

    NewCookie cookie = new NewCookie(AUTH_TOKEN_NAME, "", "/", null, null, -1, false);
    return Response.ok().cookie(cookie).build();
  }

  /**
   * If there was a valid token, the request interceptor would have set PrincipalContext already.
   * @return
   */
  @GET
  public Response getPrincipalContext() {
    // TODO refactor this, use injection
    ThirdEyePrincipal principal = ThirdEyeAuthFilter.getCurrentPrincipal();
    if (principal == null) {
      LOG.error("Could not find a valid user");
      return Response.status(Response.Status.UNAUTHORIZED).build();
    }
    return Response.ok(principal).build();
  }

  private String generateSessionKey(String principalName) {
    return DigestUtils.sha256Hex(principalName + this.random.nextLong());
  }

}