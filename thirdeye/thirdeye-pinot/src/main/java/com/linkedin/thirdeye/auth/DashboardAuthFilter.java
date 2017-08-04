package com.linkedin.thirdeye.auth;

import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.Authenticator;
import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;


public class DashboardAuthFilter extends AuthFilter<AuthRequest, PrincipalAuthContext> {

  final Authenticator<AuthRequest, PrincipalAuthContext> authenticator;

  public DashboardAuthFilter(Authenticator<AuthRequest, PrincipalAuthContext> authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    // TODO: write implementation
  }
}
