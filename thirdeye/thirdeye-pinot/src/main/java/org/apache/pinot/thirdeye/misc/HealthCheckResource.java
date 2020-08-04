package org.apache.pinot.thirdeye.misc;

import org.apache.pinot.thirdeye.datalayer.util.DaoProviderUtil;
import org.apache.pinot.thirdeye.datalayer.util.ManagerProvider;
import org.apache.pinot.thirdeye.datasource.sql.SqlResponseCacheLoader;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;

@Path("/ping")
@Produces(MediaType.APPLICATION_JSON)
public class HealthCheckResource {

    @GET
    @Path(value = "/")
    public String healthCheck() {
        if (DaoProviderUtil.isDataSourceHealthy()) {
            return "I'm healthy";
        }
        return "Unhealthy";
    }
}
