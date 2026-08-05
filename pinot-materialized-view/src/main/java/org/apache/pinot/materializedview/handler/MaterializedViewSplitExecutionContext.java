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
package org.apache.pinot.materializedview.handler;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.materializedview.context.MaterializedViewContext;
import org.apache.pinot.spi.data.Schema;


/// Inputs the broker passes to [MaterializedViewHandler#executeSplit] for a split-rewrite
/// execution. The MV handler's job is to attach the per-branch time-boundary filters and then
/// call back via the [MaterializedViewSplitDispatcher] for the actual route-build +
/// scatter-gather + reduce, so this context only carries what the handler needs to compute and
/// attach those filters.
public final class MaterializedViewSplitExecutionContext {
  private final BrokerRequest _originalBrokerRequest;
  private final PinotQuery _baseServerPinotQuery;
  private final Schema _baseSchema;
  private final MaterializedViewContext _materializedViewContext;
  private final TableRouteInfo _baseRouteInfo;
  private final long _remainingTimeMs;
  private final MaterializedViewSplitDispatcher _dispatcher;

  private MaterializedViewSplitExecutionContext(Builder b) {
    _originalBrokerRequest = b._originalBrokerRequest;
    _baseServerPinotQuery = b._baseServerPinotQuery;
    _baseSchema = b._baseSchema;
    _materializedViewContext = b._materializedViewContext;
    _baseRouteInfo = b._baseRouteInfo;
    _remainingTimeMs = b._remainingTimeMs;
    _dispatcher = b._dispatcher;
  }

  public BrokerRequest getOriginalBrokerRequest() {
    return _originalBrokerRequest;
  }

  public PinotQuery getBaseServerPinotQuery() {
    return _baseServerPinotQuery;
  }

  public Schema getBaseSchema() {
    return _baseSchema;
  }

  public MaterializedViewContext getMaterializedViewContext() {
    return _materializedViewContext;
  }

  public TableRouteInfo getBaseRouteInfo() {
    return _baseRouteInfo;
  }

  public long getRemainingTimeMs() {
    return _remainingTimeMs;
  }

  public MaterializedViewSplitDispatcher getDispatcher() {
    return _dispatcher;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private BrokerRequest _originalBrokerRequest;
    private PinotQuery _baseServerPinotQuery;
    private Schema _baseSchema;
    private MaterializedViewContext _materializedViewContext;
    private TableRouteInfo _baseRouteInfo;
    private long _remainingTimeMs;
    private MaterializedViewSplitDispatcher _dispatcher;

    public Builder originalBrokerRequest(BrokerRequest originalBrokerRequest) {
      _originalBrokerRequest = originalBrokerRequest;
      return this;
    }

    public Builder baseServerPinotQuery(PinotQuery baseServerPinotQuery) {
      _baseServerPinotQuery = baseServerPinotQuery;
      return this;
    }

    public Builder baseSchema(Schema baseSchema) {
      _baseSchema = baseSchema;
      return this;
    }

    public Builder materializedViewContext(MaterializedViewContext materializedViewContext) {
      _materializedViewContext = materializedViewContext;
      return this;
    }

    public Builder baseRouteInfo(TableRouteInfo baseRouteInfo) {
      _baseRouteInfo = baseRouteInfo;
      return this;
    }

    public Builder remainingTimeMs(long remainingTimeMs) {
      _remainingTimeMs = remainingTimeMs;
      return this;
    }

    public Builder dispatcher(MaterializedViewSplitDispatcher dispatcher) {
      _dispatcher = dispatcher;
      return this;
    }

    public MaterializedViewSplitExecutionContext build() {
      return new MaterializedViewSplitExecutionContext(this);
    }
  }
}
