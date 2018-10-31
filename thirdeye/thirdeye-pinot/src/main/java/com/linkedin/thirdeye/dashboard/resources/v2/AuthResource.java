package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Optional;
import com.linkedin.thirdeye.auth.AuthCookie;
import com.linkedin.thirdeye.auth.AuthCookieSerializer;
import com.linkedin.thirdeye.auth.Credentials;
import com.linkedin.thirdeye.auth.ThirdEyeAuthFilter;
import com.linkedin.thirdeye.auth.ThirdEyePrincipal;
import io.dropwizard.auth.Authenticator;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/auth")
@Produces(MediaType.APPLICATION_JSON)
public class AuthResource {
  public static final String AUTH_TOKEN_NAME = "te_auth";
  private static final Logger LOG = LoggerFactory.getLogger(AuthResource.class);

  private final Authenticator<Credentials, ThirdEyePrincipal> authenticator;
  private final AuthCookieSerializer serializer;
  private final long cookieTTL;

  public AuthResource(Authenticator<Credentials, ThirdEyePrincipal> authenticator, AuthCookieSerializer serializer, long cookieTTL) {
    this.authenticator = authenticator;
    this.serializer = serializer;
    this.cookieTTL = cookieTTL;
  }

  @Path("/authenticate")
  @POST
  public Response authenticate(Credentials credentials) {
    try {
      final Optional<ThirdEyePrincipal> optPrincipal = this.authenticator.authenticate(credentials);
      if (!optPrincipal.isPresent()) {
        return Response.status(Response.Status.UNAUTHORIZED).build();
      }

      final ThirdEyePrincipal principal = optPrincipal.get();

      AuthCookie authCookie = new AuthCookie();
      authCookie.setPrincipal(credentials.getPrincipal());
      authCookie.setPassword(credentials.getPassword()); // TODO replace with token in DB
      authCookie.setTimeCreated(System.currentTimeMillis());

      final String cookieValue = this.serializer.serializeCookie(authCookie);

      NewCookie cookie = new NewCookie(AUTH_TOKEN_NAME, cookieValue, "/", null, null, (int) (this.cookieTTL / 1000), false);

      return Response.ok(principal).cookie(cookie).build();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return Response.status(Response.Status.UNAUTHORIZED).build();
  }

  @Path("/logout")
  @GET
  public Response logout() {
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
      LOG.error("Could not find a valid the user");
      return Response.status(Response.Status.UNAUTHORIZED).build();
    }
    return Response.ok(principal).build();
  }
}