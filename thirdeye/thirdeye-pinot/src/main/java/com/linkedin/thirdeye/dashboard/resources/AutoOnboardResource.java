package com.linkedin.thirdeye.dashboard.resources;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.auto.onboard.AutoOnboard;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.DataSources;
import com.linkedin.thirdeye.datasource.DataSourcesLoader;


/**
 * Endpoints for triggering adhoc onboard on auto onboard services
 */
@Path(value = "/autoOnboard")
@Produces(MediaType.APPLICATION_JSON)
public class AutoOnboardResource {
  private Map<String, AutoOnboard> dataSourceToOnboardMap = new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardResource.class);

  public AutoOnboardResource(ThirdEyeConfiguration thirdeyeConfig) {
    String dataSourcesPath = thirdeyeConfig.getDataSourcesPath();
    DataSources dataSources = DataSourcesLoader.fromDataSourcesPath(dataSourcesPath);
    if (dataSources == null) {
      throw new IllegalStateException("Could not create data sources config from path " + dataSourcesPath);
    }
    for (DataSourceConfig dataSourceConfig : dataSources.getDataSourceConfigs()) {
      String autoLoadClassName = dataSourceConfig.getAutoLoadClassName();
      if (StringUtils.isNotBlank(autoLoadClassName)) {
        try {
          Constructor<?> constructor = Class.forName(autoLoadClassName).getConstructor(DataSourceConfig.class);
          AutoOnboard autoOnboardConstructor = (AutoOnboard) constructor.newInstance(dataSourceConfig);
          String datasourceClassName = dataSourceConfig.getClassName();
          String dataSource = datasourceClassName.substring(datasourceClassName.lastIndexOf(".") + 1, datasourceClassName.length());
          dataSourceToOnboardMap.put(dataSource, autoOnboardConstructor);
        } catch (Exception e) {
          LOG.error("Exception in creating autoload constructor {}", autoLoadClassName);
        }
      }
    }
  }

  @POST
  @Path("/runAdhoc/{datasource}")
  public Response runAdhocOnboard(@PathParam("datasource") String datasource) {
    if (!dataSourceToOnboardMap.containsKey(datasource)) {
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(String.format("Data source %s does not exist in config", datasource)).build();
    }
    dataSourceToOnboardMap.get(datasource).runAdhoc();
    return Response.ok().build();
  }

}
