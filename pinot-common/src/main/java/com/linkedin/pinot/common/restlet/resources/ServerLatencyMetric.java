package com.linkedin.pinot.common.restlet.resources;

/**
 * Created by Gandharv on 10/14/2017.
 */
public class ServerLatencyMetric {
    public static final int OBJ_SIZE = 8*4;
    private long _timestamp;
    private long _latency;
    private long _segments;
    private long _numRequests;
    private long _documents;

    public ServerLatencyMetric(long timestamp, long latency, long segments, long documents){
        _timestamp = timestamp;
        _segments = segments;
        _latency = latency;
        _documents = documents;
        _numRequests = 1;
    }

    public ServerLatencyMetric(){

    }

    public long getDocuments() {
        return _documents;
    }

    public void setDocuments(long _documents) {
        this._documents = _documents;
    }

    public long getLatency() {
        return _latency;
    }

    public void setLatency(Long _latency) {
        this._latency = _latency;
    }

    public long getSegmentSize() {
        return _segments;
    }

    public void setSegmentSize(Long _segmentSize) {
        this._segments = _segmentSize;
    }

    public double getAvglatency() {
        return 1.0*(_latency/_numRequests);
    }

    public double getAvgSegmentSize() {
        return 1.0*(_segments /_numRequests);
    }

    public double getAvgDocumentSize() {
        return 1.0*(_documents/_numRequests);
    }

    public long getNumRequests() {
        return _numRequests;
    }

    public void setNumRequests(long _numRequests) {
        this._numRequests = _numRequests;
    }

    public long getTimestamp() {
        return _timestamp;
    }

    public void setTimestamp(long _timestamp) {
        this._timestamp = _timestamp;
    }

    @Override
    public String toString() {
        return (this.getTimestamp() + "," + this.getNumRequests() + "," + this.getAvglatency() + "," + this.getAvgSegmentSize() + "," + this.getAvgDocumentSize() + "\n");
    }
}
