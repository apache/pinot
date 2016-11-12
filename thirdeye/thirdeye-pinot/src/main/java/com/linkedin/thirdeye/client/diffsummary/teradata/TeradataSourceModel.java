package com.linkedin.thirdeye.client.diffsummary.teradata;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.apache.tomcat.jdbc.pool.DataSource;


public class TeradataSourceModel extends AbstractModule {
  @Override
  protected void configure() {
    bind(DataSource.class)
        .annotatedWith(TeraDs.class)
        .toProvider(TeradataSourceProvider.class)
        .in(Scopes.SINGLETON);
  }
}
