package com.linkedin.pinot.common.restlet.resources;

/**
 * Created by Gandharv on 10/14/2017.
 */
public class ServerLoadMetric {
    public long timestamp;
    public Double avglatency;
    public Double  avgSegments;
    public long numRequests;
}
