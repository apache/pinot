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
package org.apache.pinot.query.planner.physical.v2;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RelToPRelConverter {
  public static final RelToPRelConverter INSTANCE = new RelToPRelConverter();
  private static final Logger LOGGER = LoggerFactory.getLogger(RelToPRelConverter.class);

  private RelToPRelConverter() {
  }

  public PRelNode toPRelNode(RelNode relNode, PhysicalPlannerContext context, Map<String, String> queryOptions,
      TableCache tableCache) {
    PRelNode pRelNode = PRelNode.wrapRelTree(relNode, context.getNodeIdGenerator());
    var rules = PhysicalOptRuleSet.create(context, queryOptions, tableCache);
    for (var ruleAndExecutor : rules) {
      PRelOptRule rule = ruleAndExecutor.getLeft();
      RuleExecutor executor = ruleAndExecutor.getRight();
      pRelNode = executor.execute(pRelNode, rule, context);
    }
    PRelNode.printWrappedRelNode(pRelNode, 0);
    return pRelNode;
  }
}
