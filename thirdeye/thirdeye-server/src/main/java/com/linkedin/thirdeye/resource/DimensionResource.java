package com.linkedin.thirdeye.resource;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.query.ThirdEyeQueryExecutor;

@Path("/dimensions")
@Produces(MediaType.APPLICATION_JSON)
public class DimensionResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionResource.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ThirdEyeQueryExecutor queryExecutor;

  public DimensionResource(ThirdEyeQueryExecutor queryExecutor) {
    this.queryExecutor = queryExecutor;
  }

  @GET
  @Path("/{collection}/{startTime}/{endTime}")
  @Timed
  public Map<String, Collection<String>> get(@PathParam("collection") String collection,
      @PathParam("startTime") Long startTime, @PathParam("endTime") Long endTime,
      @Context UriInfo uriInfo) throws Exception {
    Map<String, Collection<String>> fixedDimensions = toMap(uriInfo.getQueryParameters());

    DateTime queryStart = new DateTime(startTime);
    DateTime queryEnd = new DateTime(endTime);
    return queryExecutor.getAllDimensionValues(collection, queryStart, queryEnd, fixedDimensions);
  }

  private <K, V> Map<K, Collection<V>> toMap(MultivaluedMap<K, V> multiMap) {
    HashMap<K, Collection<V>> result = new HashMap<>(multiMap.size());
    for (Map.Entry<K, List<V>> entry : multiMap.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

}
