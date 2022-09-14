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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;


public class PinotRuleUtils {
  private static final RelBuilder.Config DEFAULT_CONFIG =
      RelBuilder.Config.DEFAULT.withAggregateUnique(true).withPushJoinCondition(true);

  public static final RelBuilderFactory PINOT_REL_FACTORY =
      RelBuilder.proto(Contexts.of(RelFactories.DEFAULT_STRUCT, DEFAULT_CONFIG));

  private PinotRuleUtils() {
    // do not instantiate.
  }

  public static boolean isExchange(RelNode rel) {
    RelNode reference = rel;
    if (reference instanceof HepRelVertex) {
      reference = ((HepRelVertex) reference).getCurrentRel();
    }
    return reference instanceof Exchange;
  }
}
