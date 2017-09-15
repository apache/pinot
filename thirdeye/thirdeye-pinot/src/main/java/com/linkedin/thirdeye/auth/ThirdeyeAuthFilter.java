package com.linkedin.thirdeye.auth;

import com.linkedin.thirdeye.dashboard.configs.AuthConfiguration;
import com.linkedin.thirdeye.dashboard.resources.v2.AuthResource;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.Authenticator;
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.Response;
import com.google.common.base.Optional;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdeyeAuthFilter extends AuthFilter<AuthRequest, PrincipalAuthContext> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdeyeAuthFilter.class);
  private final Authenticator<AuthRequest, PrincipalAuthContext> authenticator;
  private final AuthConfiguration authConfig;
  public ThirdeyeAuthFilter(Authenticator<AuthRequest, PrincipalAuthContext> authenticator, AuthConfiguration authConfig) {
    this.authenticator = authenticator;
    this.authConfig = authConfig;
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    String uriPath = containerRequestContext.getUriInfo().getPath();
    if (!authConfig.isAuthEnabled()
        // authenticate end points should be out of auth filter
        || uriPath.equals("auth/authenticate")|| uriPath.equals("auth/logout")
        // Landing page should not throw 401
        || uriPath.equals("thirdeye")
        // Let the FE handle the redirect to login page when not authenticated
        || uriPath.equals("thirdeye-admin")
        // Let detector capture the screenshot without authentication error
        || uriPath.startsWith("/app/#/screenshot/")
        || uriPath.startsWith("anomalies/search/anomalyIds")

        // at last any other path specified in the te-config
        || authConfig.getAllowedPaths().contains(uriPath)) {
      return;
    }
    Optional<PrincipalAuthContext> authenticatedUser;
    try {
      AuthRequest credentials = getCredentials(containerRequestContext);
      authenticatedUser = authenticator.authenticate(credentials);
    } catch (Exception e) {
      throw new WebApplicationException("Unable to validate credentials", Response.Status.UNAUTHORIZED);
    }
    if (!authenticatedUser.isPresent() || Strings.isNullOrEmpty(authenticatedUser.get().getName())) {
      throw new WebApplicationException("Credentials not valid", Response.Status.UNAUTHORIZED);
    }
    LOG.info("authenticated user: {}, access end point: {}", authenticatedUser.get().getName(), uriPath);
  }

  private AuthRequest getCredentials(ContainerRequestContext requestContext) {
    AuthRequest credentials = new AuthRequest();
    try {
      Map<String, Cookie> cookies = requestContext.getCookies();
      if (cookies != null) {
        String rawToken = cookies.get(AuthResource.AUTH_TOKEN_NAME).getValue();
        credentials.setToken(rawToken);
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
      throw new WebApplicationException("Unable to parse credentials", Response.Status.UNAUTHORIZED);
    }
    return credentials;
  }
}
