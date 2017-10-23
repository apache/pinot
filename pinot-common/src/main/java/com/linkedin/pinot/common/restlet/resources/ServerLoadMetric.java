package com.linkedin.pinot.common.restlet.resources;

import com.linkedin.pinot.common.utils.CommonConstants;

/**
 * Created by Gandharv on 10/14/2017.
 */
public class ServerLoadMetric {
    public long _timestamp;
    public Double _avglatency;
    public Double  _avgSegments;
    public long _numRequests;

    public ServerLoadMetric(long timestamp, Double avglatency, Double avgSegments){
        _timestamp = timestamp;
        _avgSegments = avgSegments;
        _avglatency = avglatency;
    }
}
