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

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.sql.SqlKind;


/**
 * Customized rule extends {@link FilterJoinRule.FilterIntoJoinRule}, since Pinot only support equality JOIN.
 */
public class PinotFilterIntoJoinRule extends FilterJoinRule.FilterIntoJoinRule {
  public static final PinotFilterIntoJoinRule INSTANCE = new PinotFilterIntoJoinRule();
  protected PinotFilterIntoJoinRule() {
    super(ImmutableFilterIntoJoinRuleConfig.of((join, joinType, exp) ->
            exp.getKind() == SqlKind.AND || exp.getKind() == SqlKind.EQUALS)
        .withOperandSupplier(b0 ->
            b0.operand(Filter.class).oneInput(b1 ->
                b1.operand(Join.class).anyInputs()))
        .withSmart(true));
  }
}
