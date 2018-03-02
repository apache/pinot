package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces(MediaType.APPLICATION_JSON)
public class OverrideConfigResource {

  private static final Logger LOG = LoggerFactory.getLogger(OverrideConfigResource.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();


}
