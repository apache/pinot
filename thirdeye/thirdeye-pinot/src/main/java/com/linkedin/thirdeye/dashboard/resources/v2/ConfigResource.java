package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.config.ConfigNamespace;
import com.linkedin.thirdeye.datalayer.bao.ConfigManager;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path(value = "/config")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigResource {
  private final static Logger LOG = LoggerFactory.getLogger(ConfigResource.class);

  private final ConfigManager configDAO;

  public ConfigResource(ConfigManager configDAO) {
    this.configDAO = configDAO;
  }

  @GET
  @Path("/{namespace}/{name}")
  public Object get(
      @PathParam("namespace") String namespace,
      @PathParam("name") String name) {
    return makeNamespace(namespace).get(name);
  }

  @POST
  @Path("/{namespace}/{name}")
  public void put(
      @PathParam("namespace") String namespace,
      @PathParam("name") String name,
      String payload) throws IOException {

    ConfigNamespace cn = makeNamespace(namespace);

    // determine suitable representation

    // map
    try {
      cn.put(name, new ObjectMapper().readValue(payload, new TypeReference<Map<String, ?>>() {}));
      LOG.info("Storing MAP '{}':'{}' = '{}'", namespace, name, payload);
      return;
    } catch (Exception ignore) {
      // left blank
    }

    // list
    try {
      cn.put(name, new ObjectMapper().readValue(payload, new TypeReference<List<?>>() {}));
      LOG.info("Storing LIST '{}':'{}' = '{}'", namespace, name, payload);
      return;
    } catch (Exception ignore) {
      // left blank
    }

    // string
    cn.put(name, payload);
    LOG.info("Storing STRING '{}':'{}' = '{}'", namespace, name, payload);

  }

  @GET
  @Path("/{namespace}/")
  public Map<String, Object> list(
      @PathParam("namespace") String namespace) {
    return makeNamespace(namespace).getAll();
  }

  @DELETE
  @Path("/{namespace}/{name}")
  public void delete(
      @PathParam("namespace") String namespace,
      @PathParam("name") String name) throws IOException {
    makeNamespace(namespace).delete(name);
  }

  private ConfigNamespace makeNamespace(String namespace) {
    return new ConfigNamespace(namespace, this.configDAO);
  }
}
