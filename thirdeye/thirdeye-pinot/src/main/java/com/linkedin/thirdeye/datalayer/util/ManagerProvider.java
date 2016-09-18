package com.linkedin.thirdeye.datalayer.util;

import org.apache.tomcat.jdbc.pool.DataSource;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AbstractManagerImpl;
import com.linkedin.thirdeye.datalayer.dao.GenericPojoDao;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil.DataSourceModule;

public class ManagerProvider {

  Injector injector;
  DataSource dataSource;
  private GenericPojoDao genericPojoDao;

  public ManagerProvider(DataSource dataSource) {
    this.dataSource = dataSource;
    DataSourceModule dataSourceModule = new DataSourceModule(dataSource);
    injector = Guice.createInjector(dataSourceModule);
    genericPojoDao = injector.getInstance(GenericPojoDao.class);
  }

  public <T extends AbstractManagerImpl<? extends AbstractDTO>> T getInstance(Class<T> c) {
    T manager = injector.getInstance(c);
    manager.setGenericPojoDao(genericPojoDao);
    return manager;
  }


}
