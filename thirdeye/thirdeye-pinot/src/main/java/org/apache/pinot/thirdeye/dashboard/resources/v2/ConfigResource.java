/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import com.google.inject.Inject;
import org.apache.pinot.thirdeye.config.ConfigNamespace;
import org.apache.pinot.thirdeye.datalayer.bao.ConfigManager;
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

  @Inject
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
    LOG.warn("Call to a deprecated end point " + "/config/{namespace}/ " + getClass().getName());
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
