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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This class gives the details of a particular schema and the corresponding column count metrics
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaInfo {
  @JsonProperty("schemaName")
  private String _schemaName;

  @JsonProperty("numDimensionFields")
  private int _numDimensionFields;

  @JsonProperty("numDateTimeFields")
  private int _numDateTimeFields;

  @JsonProperty("numMetricFields")
  private int _numMetricFields;

  @JsonProperty("numComplexFields")
  private int _numComplexFields;

  public String getSchemaName() {
    return _schemaName;
  }

  public int getNumDimensionFields() {
    return _numDimensionFields;
  }

  public int getNumDateTimeFields() {
    return _numDateTimeFields;
  }

  public int getNumMetricFields() {
    return _numMetricFields;
  }

  public int getNumComplexFields() {
    return _numComplexFields;
  }

  public SchemaInfo() {
  }

  public SchemaInfo(Schema schema) {
    _schemaName = schema.getSchemaName();

    //Removed virtual columns($docId, $hostName, $segmentName) from dimension fields count
    _numDimensionFields = schema.getDimensionFieldSpecs().size() - 3;
    _numDateTimeFields = schema.getDateTimeFieldSpecs().size();
    _numMetricFields = schema.getMetricFieldSpecs().size();
    _numComplexFields = schema.getComplexFieldSpecs().size();
  }
}
