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

import java.util.ArrayList;
import java.util.List;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerLatencyInfo {
    public String _serverName;
    public String _tableName;
    public List<Double> _segmentLatencyInSecs;

    public ServerLatencyInfo() {
        //We set default value to -1 as indication of error (e.g., timeout) in returning latency info from a Pinot server;
        _segmentLatencyInSecs = new ArrayList<Double>();
    }

    public String get_serverName() {
        return _serverName;
    }

    public void set_serverName(String sName) {
        _serverName = sName;
    }

    public String get_tableName() {
        return _tableName;
    }
    public void set_tableName(String _tableName) {
        this._tableName = _tableName;
    }

    public List<Double> get_segmentLatencyInSecs() {
        return _segmentLatencyInSecs;
    }

    public void set_segmentLatencyInSecs(List<Double> segmentLatencyInSecs) {
        _segmentLatencyInSecs = segmentLatencyInSecs;
    }


}