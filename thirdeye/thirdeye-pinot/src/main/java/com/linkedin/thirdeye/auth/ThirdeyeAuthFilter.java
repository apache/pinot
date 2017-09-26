package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;
import com.linkedin.thirdeye.dashboard.resources.v2.AuthResource;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdeyeAuthFilter extends AuthFilter<Credentials, ThirdEyePrincipal> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdeyeAuthFilter.class);

  private final Authenticator<Credentials, ThirdEyePrincipal> authenticator;
  private final AuthCookieSerializer serializer;
  private final Set<String> allowedPaths;

  public ThirdeyeAuthFilter(Authenticator<Credentials, ThirdEyePrincipal> authenticator, AuthCookieSerializer serializer, Set<String> allowedPaths) {
    this.authenticator = authenticator;
    this.serializer = serializer;
    this.allowedPaths = allowedPaths;
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    String uriPath = containerRequestContext.getUriInfo().getPath();
    // authenticate end points should be out of auth filter
    if (uriPath.equals("auth/authenticate") || uriPath.equals("auth/logout")
        // Landing page should not throw 401
        || uriPath.equals("thirdeye")
        // Let the FE handle the redirect to login page when not authenticated
        || uriPath.equals("thirdeye-admin")
        // Let detector capture the screenshot without authentication error
        || uriPath.startsWith("anomalies/search/anomalyIds") || uriPath.startsWith("thirdeye/email/generate/datasets")) {
      return;
    }

    for (String fragment : this.allowedPaths) {
      if (uriPath.startsWith(fragment)) {
        return;
      }
    }

    Credentials credentials;
    try {
      credentials = getCredentials(containerRequestContext);
    } catch(Exception e) {
      LOG.error("Error while extracting credentials from cookie");
      throw new WebApplicationException("Unable to parse credentials", Response.Status.UNAUTHORIZED);
    }

    try {
      Optional<ThirdEyePrincipal> authenticatedUser = authenticator.authenticate(credentials);
      if (!authenticatedUser.isPresent()) {
        throw new WebApplicationException("Unable to validate credentials", Response.Status.UNAUTHORIZED);
      }
    } catch (AuthenticationException e) {
      LOG.error("Error while authenticating credentials for {}", credentials.getPrincipal());
      throw new WebApplicationException("Error while authenticating credentials", Response.Status.UNAUTHORIZED);
    }
  }

  private Credentials getCredentials(ContainerRequestContext requestContext) throws Exception {
    Credentials credentials = new Credentials();

    Map<String, Cookie> cookies = requestContext.getCookies();
    if (cookies != null && cookies.containsKey(AuthResource.AUTH_TOKEN_NAME)) {
      String rawCookie = cookies.get(AuthResource.AUTH_TOKEN_NAME).getValue();
      AuthCookie cookie = this.serializer.deserializeCookie(rawCookie);
      credentials.setPrincipal(cookie.getPrincipal());
      credentials.setPassword(cookie.getPassword()); // TODO replace with token in DB
    }

    return credentials;
  }
}
