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
package org.apache.pinot.plugin.metrics.compound;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.spi.metrics.PinotMetric;


public abstract class AbstractCompoundPinotMetric<M extends PinotMetric> implements PinotMetric {
  protected final List<M> _metrics;

  public AbstractCompoundPinotMetric(List<M> metrics) {
    Preconditions.checkArgument(!metrics.isEmpty(), "At least one meter is needed");
    _metrics = metrics;
  }

  @Override
  public Object getMetric() {
    return _metrics.stream().map(PinotMetric::getMetric).collect(Collectors.toList());
  }

  protected M getSomeMeter() {
    return _metrics.get(0);
  }

  public M getMeter(int index) {
    return _metrics.get(index);
  }
}
