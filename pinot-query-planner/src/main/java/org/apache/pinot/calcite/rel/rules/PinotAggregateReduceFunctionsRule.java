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
package org.apache.pinot.calcite.rel.rules;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.sql.SqlKind;


/**
 * Pinot customized version of {@link AggregateReduceFunctionsRule} which only reduce on SUM and AVG.
 * We don't want to reduce on STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, COVAR_POP, COVAR_SAMP because Pinot supports
 * them natively, but not REGR_COUNT which can be generated during reduce.
 */
public class PinotAggregateReduceFunctionsRule extends AggregateReduceFunctionsRule {
  public static final PinotAggregateReduceFunctionsRule INSTANCE = new PinotAggregateReduceFunctionsRule(
      (Config) Config.DEFAULT.withRelBuilderFactory(PinotRuleUtils.PINOT_REL_FACTORY));

  private PinotAggregateReduceFunctionsRule(Config config) {
    super(config);
  }

  @Override
  public boolean canReduce(AggregateCall call) {
    SqlKind kind = call.getAggregation().getKind();
    return kind == SqlKind.SUM || kind == SqlKind.AVG;
  }
}
