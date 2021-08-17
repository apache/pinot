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

package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Container object to encapsulate table status which contains
 * - Ingestion status: specifies if the table is ingesting properly or not
 */
public class TableStatus {

  private IngestionStatus _ingestionStatus;

  public enum IngestionState {
    HEALTHY, UNHEALTHY, UNKNOWN
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class IngestionStatus {
    private IngestionState _ingestionState;
    private String _errorMessage;

    public IngestionStatus(@JsonProperty("ingestionState") String ingestionState, @JsonProperty("errorMessage") String errorMessage) {
      _ingestionState = IngestionState.valueOf(ingestionState);
      _errorMessage = errorMessage;
    }

    public static IngestionStatus newIngestionStatus(IngestionState state, String errorMessage) {
      return new IngestionStatus(state.toString(), errorMessage);
    }

    public IngestionState getIngestionState() {
      return _ingestionState;
    }

    public String getErrorMessage() {
      return _errorMessage;
    }
  }

  public TableStatus(@JsonProperty("ingestionStatus") IngestionStatus ingestionStatus) {
    _ingestionStatus = ingestionStatus;
  }

  public IngestionStatus getIngestionStatus() {
    return _ingestionStatus;
  }
}
