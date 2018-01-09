/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.restlet.resources;

public class ServerLatencyMetric {
    public static final int OBJ_SIZE = 8*4;
    private long _timestamp;
    private long _latency;
    private long _segments;
    private long _segment_size;
    private long _numRequests = 1;
    private long _documents;
    private String _tableName = "";

    public ServerLatencyMetric(long timestamp, long latency, long segments, long documents, long segment_size){
        _timestamp = timestamp;
        _segments = segments;
        _latency = latency;
        _documents = documents;
        _segment_size = segment_size;
        _numRequests = 1;
    }

    public ServerLatencyMetric(){

    }

    public ServerLatencyMetric(ServerLatencyMetric metric) {
        _timestamp = metric.getTimestamp();
        _segments = metric.getSegmentCount();
        _latency = metric.getLatency();
        _documents = metric.getDocuments();
        _segment_size = metric.getSegmentSize();
        _numRequests = metric.getNumRequests();
    }

    public String get_tableName() {
        return _tableName;
    }

    public void set_tableName(String _tableName) {
        this._tableName = _tableName;
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
        return _segment_size;
    }

    public void setSegmentSize(Long _segmentSize) {
        this._segment_size = _segmentSize;
    }

    public long getSegmentCount() {
        return _segments;
    }

    public void setSegmentCount(Long _segments) {
        this._segments = _segments;
    }

    public double getAvglatency() {
        return 1.0*(_latency/_numRequests);
    }

    public double getAvgSegmentCount() {
        return 1.0*(_segments /_numRequests);
    }

    public double getAvgSegmentSize() {
        return 1.0*(_segment_size /_numRequests);
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
        return (this.getTimestamp() + "," + this.getNumRequests() + "," + this.getAvglatency() + "," + this.getAvgSegmentSize() + "," + this.getAvgSegmentCount() + "," + this.getAvgDocumentSize() + "\n");
    }
}
