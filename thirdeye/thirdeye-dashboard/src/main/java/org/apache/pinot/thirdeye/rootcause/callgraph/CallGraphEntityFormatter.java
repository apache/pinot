/*
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

package org.apache.pinot.thirdeye.rootcause.callgraph;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import org.apache.pinot.thirdeye.rootcause.Entity;


public class CallGraphEntityFormatter extends RootCauseEntityFormatter {
  private static final String ATTR_INDEX = "index";
  private static final String ATTR_DIMENSIONS = "dimensions";

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof CallGraphEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    CallGraphEntity e = (CallGraphEntity) entity;

    Multimap<String, String> attributes = ArrayListMultimap.create();

    for (String seriesName : e.getEdge().getSeriesNames()) {
      attributes.put(seriesName, e.getEdge().getString(seriesName, 0));
    }
    attributes.putAll(ATTR_DIMENSIONS, e.getEdge().getSeriesNames());
    attributes.putAll(ATTR_INDEX, e.getEdge().getIndexNames());

    RootCauseEntity out = makeRootCauseEntity(entity, "callgraph", "(callgraph edge)", "");

    out.setAttributes(attributes);

    return out;
  }
}
