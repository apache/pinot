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
package org.apache.pinot.query.planner.explain;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Extends {@link RelJsonWriter} to add the type of the relational algebra node.
 */
public class PinotRelJsonWriter extends RelJsonWriter {

  public PinotRelJsonWriter() {
    super(new JsonBuilder(), relJson -> new PinotRelJson(relJson));
  }

  @Override
  protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
    super.explain_(rel, values);

    Object last = relList.get(relList.size() - 1);
    if (last instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) last;
      map.put("type", rel.getRelTypeName());
    }
  }

  static class PinotRelJson extends RelJson {
    private final RelJson _relJson;

    /**
     * Creates a PinotRelJson.
     */
    public PinotRelJson(RelJson relJson) {
      super(null);
      _relJson = relJson;
    }

    @Override
    public @Nullable Object toJson(@Nullable Object value) {
      if (value instanceof AggregateNode.AggType) {
        return ((AggregateNode.AggType) value).name();
      }
      return _relJson.toJson(value);
    }
  }
}
