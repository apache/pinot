package com.linkedin.thirdeye.auth;

import com.linkedin.thirdeye.dashboard.resources.v2.AuthResource;
import io.dropwizard.auth.Authenticator;
import java.io.IOException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import com.google.common.base.Optional;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdeyeAuthFilter extends io.dropwizard.auth.AuthFilter {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdeyeAuthFilter.class);
  final boolean enableFilter;
  final Authenticator<AuthRequest, PrincipalAuthContext> authenticator;

  public ThirdeyeAuthFilter(Authenticator<AuthRequest, PrincipalAuthContext> authenticator, boolean enableFilter) {
    this.authenticator = authenticator;
    this.enableFilter = enableFilter;
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    String uriPath = containerRequestContext.getUriInfo().getPath();
    if (!enableFilter || uriPath.startsWith("auth") || uriPath.equals("thirdeye")) {
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
      String rawToken = requestContext.getCookies().get(AuthResource.AUTH_TOKEN_NAME).getValue();
      credentials.setToken(rawToken);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
      throw new WebApplicationException("Unable to parse credentials", Response.Status.UNAUTHORIZED);
    }
    return credentials;
  }
}
