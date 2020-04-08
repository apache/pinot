/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Collections;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
/**
 * class to wrap around pinot upsert low-water-mark information that got pass between pinot server and broker
 */
public class TableLowWaterMarksInfo {
    // A mapping from table name to the table's partiton -> low_water_mark mappings.
    private Map<String, Map<Integer, Long>> tableLowWaterMarks;

    /**
     * @param tableLowWaterMarks mapping for {<table_name>: {<partition>: <low-water-mar>}}
     * example would be
     * {upsert_table_1: {0: 100, 1: 200},
     *  upsert_table_2: {0: 500}}
     */
    public TableLowWaterMarksInfo(@JsonProperty("lowWaterMarks") Map<String, Map<Integer, Long>> tableLowWaterMarks) {
        this.tableLowWaterMarks = tableLowWaterMarks;
    }

    public void setTableLowWaterMarks(Map<String, Map<Integer, Long>> tableLowWaterMarks) {
        this.tableLowWaterMarks = tableLowWaterMarks;
    }

    public Map<String, Map<Integer, Long>> getTableLowWaterMarks() {
        return Collections.unmodifiableMap(tableLowWaterMarks);
    }
}
