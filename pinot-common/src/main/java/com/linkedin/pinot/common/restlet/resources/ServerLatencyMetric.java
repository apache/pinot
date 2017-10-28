package com.linkedin.pinot.common.restlet.resources;

import com.linkedin.pinot.common.utils.CommonConstants;

/**
 * Created by Gandharv on 10/14/2017.
 */
public class ServerLatencyMetric {
    public long _timestamp;
    public Double _avglatency;
    public Double  _avgSegments;
    public long _numRequests;

    public ServerLatencyMetric(long timestamp, Double avglatency, Double avgSegments){
        _timestamp = timestamp;
        _avgSegments = avgSegments;
        _avglatency = avglatency;
    }

    public ServerLatencyMetric(){

    }

    public Double get_avglatency() {
        return _avglatency;
    }

    public void set_avglatency(Double _avglatency) {
        this._avglatency = _avglatency;
    }

    public Double get_avgSegments() {
        return _avgSegments;
    }

    public void set_avgSegments(Double _avgSegments) {
        this._avgSegments = _avgSegments;
    }

    public long get_numRequests() {
        return _numRequests;
    }

    public void set_numRequests(long _numRequests) {
        this._numRequests = _numRequests;
    }

    public long get_timestamp() {
        return _timestamp;
    }

    public void set_timestamp(long _timestamp) {
        this._timestamp = _timestamp;
    }
}
