package com.linkedin.thirdeye.dashboard.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path(value = "/hello")
public class HelloResource {


    public HelloResource() {
    }

    @GET
    @Path("/{name}")
    public String sayHello(@PathParam("name") String name) {
        return "Hello " + name;
    }

}
