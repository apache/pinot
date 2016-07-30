package com.linkedin.thirdeye.common.persistence;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PersistenceApp extends Application<PersistenceConfig> {

  static final String USAGE = "Please pass either of the arguments : \n "
      + "1: db status <config_dir> \n 2: db dump <config_dir> \n 3: db migrate <config_dir> \n";

  @Override
  public String getName() {
    return "Thirdeye DB Migration tool";
  }

  @Override
  public void initialize(final Bootstrap<PersistenceConfig> bootstrap) {
    bootstrap.addBundle(new MigrationsBundle<PersistenceConfig>() {
      public DataSourceFactory getDataSourceFactory(PersistenceConfig config) {
        DataSourceFactory dataSourceFactory = new DataSourceFactory();
        Map<String, String> props = config.getDatabaseConfiguration().getProperties();
        dataSourceFactory.setDriverClass(props.get("hibernate.connection.driver_class"));
        dataSourceFactory.setUrl(config.getDatabaseConfiguration().getUrl());
        dataSourceFactory.setPassword(config.getDatabaseConfiguration().getPassword());
        dataSourceFactory.setUser(config.getDatabaseConfiguration().getUser());
        return dataSourceFactory;
      }
    });
    bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
  }

  @Override
  public void run(final PersistenceConfig config, final Environment environment)
      throws Exception {

  }

  /**
   * Used to handle database migrations. Arguments are passed along with the
   * <code>persistence.yml</code> file. As an example, one could pass in the arguments: <br> <br>
   * <code>db migrate <i>config_folder</i></code> <br> <br> to run database migrations using
   * configurations in <code>persistence.yml</code>.
   *
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    List<String> argList = new ArrayList<>(Arrays.asList(args));
    if(argList.size() == 0) {
      System.out.println(USAGE);
      System.exit(0);
    }
    int lastIndex = argList.size() - 1;
    String thirdEyeConfigDir = argList.get(lastIndex);
    String dbConfigFile = thirdEyeConfigDir + "/" + "persistence.yml";
    argList.set(lastIndex, dbConfigFile);
    new PersistenceApp().run(argList.toArray(new String[argList.size()]));
  }
}
