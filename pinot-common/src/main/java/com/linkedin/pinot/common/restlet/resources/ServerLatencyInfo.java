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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerLatencyInfo {
    private String _serverName;
    private double _segmentLatencyInSecs;

    public ServerLatencyInfo() {
        //We set default value to -1 as indication of error (e.g., timeout) in returning latency info from a Pinot server;
        _segmentLatencyInSecs = -1;
    }

    public String getServerName() {
        return _serverName;
    }

    public void setServerName(String sName) {
        _serverName = sName;
    }

    public double getSegmentLatencyInSecs() {
        return _segmentLatencyInSecs;
    }

    public void setSegmentLatencyInSecs(double segmentLatencyInSecs) {
        _segmentLatencyInSecs = segmentLatencyInSecs;
    }
}