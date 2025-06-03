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
package org.apache.pinot.query.context;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.logical.LogicalPlanner;
import org.apache.pinot.query.validate.Validator;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * PlannerContext is an object that holds all contextual information during planning phase.
 *
 * TODO: currently we don't support option or query rewrite.
 * It is used to hold per query context for query planning, which cannot be shared across queries.
 */
public class PlannerContext implements AutoCloseable {
  private final PlannerImpl _planner;

  private final SqlValidator _validator;

  private final RelOptPlanner _relOptPlanner;
  private final LogicalPlanner _relTraitPlanner;

  private final Map<String, String> _options;
  private final Map<String, String> _plannerOutput;
  private final SqlExplainFormat _sqlExplainFormat;
  @Nullable
  private final PhysicalPlannerContext _physicalPlannerContext;

  public PlannerContext(FrameworkConfig config, Prepare.CatalogReader catalogReader, RelDataTypeFactory typeFactory,
      HepProgram optProgram, HepProgram traitProgram, Map<String, String> options, QueryEnvironment.Config envConfig,
      SqlExplainFormat sqlExplainFormat, @Nullable PhysicalPlannerContext physicalPlannerContext) {
    _planner = new PlannerImpl(config);
    _validator = new Validator(config.getOperatorTable(), catalogReader, typeFactory);
    _relOptPlanner = new LogicalPlanner(optProgram, Contexts.EMPTY_CONTEXT, config.getTraitDefs());
    _relTraitPlanner = new LogicalPlanner(traitProgram, Contexts.of(envConfig),
        Collections.singletonList(RelDistributionTraitDef.INSTANCE));
    _options = options;
    _plannerOutput = new HashMap<>();
    _sqlExplainFormat = sqlExplainFormat;
    _physicalPlannerContext = physicalPlannerContext;
  }

  public PlannerImpl getPlanner() {
    return _planner;
  }

  public SqlValidator getValidator() {
    return _validator;
  }

  public RelOptPlanner getRelOptPlanner() {
    return _relOptPlanner;
  }

  public LogicalPlanner getRelTraitPlanner() {
    return _relTraitPlanner;
  }

  public Map<String, String> getOptions() {
    return _options;
  }

  /**
   * Checks rule enabling options and set ruleFlags correspondingly.
   * The ruleFlags is passed to {@link QueryEnvironment}'s getOptProgram
   * to decide which rules are enabled / disabled dynamically.
   *
   * Currently, this applies to all rules used in optProgram.
   * All rules are enabled by default, and disabled if skipXXX option is set to true
   *
   * @param options options from sqlNodeAndOptions
   * @return the ruleFlags map used by getOptProgram of {@link QueryEnvironment}
   */
  public static Map<String, Boolean> getRuleFlags(@Nullable Map<String, String> options) {
    // To disable a rule, put {className, false} into ruleFlags
    Map<String, Boolean> ruleFlags = new HashMap<>();
    // iterate through {@link CommonConstants.Broker.Request.QueryOptionKey.RuleOptionKey}
    try {
      for (Field declaredField : CommonConstants.Broker.PlannerRules.class.getDeclaredFields()) {
        if (!declaredField.getType().equals(String.class)) {
          continue;
        }
        int mods = declaredField.getModifiers();
        if (!(Modifier.isStatic(mods) && Modifier.isFinal(mods))) {
          continue;
        }
        // the class name
        String ruleName = (String) declaredField.get(null);
        // check if rule is disabled, put {className, false} if is disabled
        if (isRuleDisabled(CommonConstants.Broker.PLANNER_RULE_SKIP + ruleName, false, options)) {
          ruleFlags.put(ruleName, false);
        }
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot get RuleOptionKeys constants", e);
    }
    return ruleFlags;
  }

  /**
   * checks if rule is disabled
   * @param ruleOptionKey corresponding QueryOptionKey of the rule
   * @param defaultDisabled is the rule enabled by default
   * @param options parsed queryOptions, or null
   * @return whether the rule is enabled
   */
  private static boolean isRuleDisabled(String ruleOptionKey, Boolean defaultDisabled,
      @Nullable Map<String, String> options) {
    if (options == null) {
      return defaultDisabled;
    }
    return Boolean.parseBoolean(options.getOrDefault(ruleOptionKey, defaultDisabled.toString()));
  }

  @Override
  public void close() {
    _planner.close();
  }

  public Map<String, String> getPlannerOutput() {
    return _plannerOutput;
  }

  public SqlExplainFormat getSqlExplainFormat() {
    return _sqlExplainFormat;
  }

  @Nullable
  public PhysicalPlannerContext getPhysicalPlannerContext() {
    return _physicalPlannerContext;
  }

  public boolean isUsePhysicalOptimizer() {
    return _physicalPlannerContext != null;
  }
}
