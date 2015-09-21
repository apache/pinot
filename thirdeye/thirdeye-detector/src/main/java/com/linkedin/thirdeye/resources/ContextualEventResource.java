package com.linkedin.thirdeye.resources;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.ContextualEvent;
import com.linkedin.thirdeye.db.ContextualEventDAO;
import io.dropwizard.hibernate.UnitOfWork;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

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
    Long id = dao.create(anomalyResult);
    return Response.ok(id).build();
  }

  @DELETE
  @Timed
  @UnitOfWork
  @Path("/{id}")
  public Response delete(@PathParam("id") Long id) {
    dao.delete(id);
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
