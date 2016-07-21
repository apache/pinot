package com.linkedin.thirdeye.detector.resources;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.detector.db.entity.ContextualEvent;
import com.linkedin.thirdeye.detector.db.dao.ContextualEventDAO;

import io.dropwizard.hibernate.UnitOfWork;

@Path("/contextual-events")
@Produces(MediaType.APPLICATION_JSON)
public class ContextualEventResource {
  private final ContextualEventDAO dao;

  public ContextualEventResource(ContextualEventDAO dao) {
    this.dao = dao;
  }

  @POST
  @Timed
  @UnitOfWork
  public Response create(ContextualEvent anomalyResult) {
    Long id = dao.save(anomalyResult);
    return Response.ok(id).build();
  }

  @DELETE
  @Timed
  @UnitOfWork
  @Path("/{id}")
  public Response delete(@PathParam("id") Long id) {
    dao.deleteById(id);
    return Response.noContent().build();
  }

  @GET
  @Timed
  @UnitOfWork
  @Path("/{startIsoTime}")
  public List<ContextualEvent> find(@PathParam("startIsoTime") String startIsoTime) {
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startIsoTime);
    DateTime endTime = DateTime.now();
    return dao.findAllByTime(startTime, endTime);
  }

  @GET
  @Timed
  @UnitOfWork
  @Path("/{startIsoTime}/{endIsoTime}")
  public List<ContextualEvent> find(@PathParam("startIsoTime") String startIsoTime,
      @PathParam("endIsoTime") String endIsoTime) {
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startIsoTime);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endIsoTime);
    return dao.findAllByTime(startTime, endTime);
  }
}
