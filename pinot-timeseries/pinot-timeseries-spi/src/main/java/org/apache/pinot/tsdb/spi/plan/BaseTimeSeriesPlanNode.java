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
package org.apache.pinot.tsdb.spi.plan;

import java.util.List;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;


/**
 * Generic plan node for time series queries. This allows each time-series query language to define their own plan
 * nodes, which in turn generate the language specific {@link BaseTimeSeriesOperator}.
 */
public abstract class BaseTimeSeriesPlanNode {
  protected final String _id;
  protected final List<BaseTimeSeriesPlanNode> _inputs;

  public BaseTimeSeriesPlanNode(String id, List<BaseTimeSeriesPlanNode> inputs) {
    _id = id;
    _inputs = inputs;
  }

  public String getId() {
    return _id;
  }

  public List<BaseTimeSeriesPlanNode> getInputs() {
    return _inputs;
  }

  public void addInputNode(BaseTimeSeriesPlanNode planNode) {
    _inputs.add(planNode);
  }

  public abstract BaseTimeSeriesPlanNode withInputs(List<BaseTimeSeriesPlanNode> newInputs);

  public abstract String getKlass();

  public abstract String getExplainName();

  public abstract BaseTimeSeriesOperator run();
}
