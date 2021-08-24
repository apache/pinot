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
package org.apache.pinot.controller.recommender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.RulesToExecute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the runner class for the rule engine, it parses the input json and maps it to a input manager,
 * Then according to the _recommend* flags set in the RulesToExecute, the engine will call the corresponding rules
 * constructed by RuleFactory
 */
public class RecommenderDriver {
  private RecommenderDriver() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RecommenderDriver.class);
  private static final String RULE_EXECUTION_PREFIX = "isRecommend";
  private static final String RULE_EXECUTION_SUFFIX = "Rule";

  public static String run(String inputJson)
      throws InvalidInputException, IOException {

    InputManager inputManager;
    ConfigManager outputManager;
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    inputManager = objectMapper.readValue(inputJson, InputManager.class);
    inputManager.init();
    outputManager = inputManager.getOverWrittenConfigs();

    // silent rules will run, but their output will only be used in other rules and it will not be present to user
    List<AbstractRule> silentRules = new ArrayList<>();

    for (RulesToExecute.Rule rule : RulesToExecute.Rule.values()) {
      try {
        Method ruleExecuteFlag = inputManager.getRulesToExecute().getClass()
            .getDeclaredMethod(RULE_EXECUTION_PREFIX + rule.name().replace(RULE_EXECUTION_SUFFIX, ""));
        LOGGER.info("{}:{}", ruleExecuteFlag.getName(), ruleExecuteFlag.invoke(inputManager.getRulesToExecute()));
        boolean shouldRun = (boolean) ruleExecuteFlag.invoke(inputManager.getRulesToExecute());
        boolean shouldSilentlyRun = false;
        if (!shouldRun) {
          shouldSilentlyRun = shouldSilentlyRun(rule, inputManager);
          if (!shouldSilentlyRun) {
            continue;
          }
        }
        AbstractRule abstractRule = RulesToExecute.RuleFactory.getRule(rule, inputManager, outputManager);
        if (abstractRule != null) {
          abstractRule.run();
          if (shouldSilentlyRun) {
            silentRules.add(abstractRule);
          }
        }
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        LOGGER.error("Error while executing strategy:{}", rule, e);
      }
    }
    try {
      silentRules.forEach(AbstractRule::hideOutput);
      return objectMapper.writeValueAsString(outputManager);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error while writing the output json string! Stack trace:", e);
    }
    return "";
  }

  private static boolean shouldSilentlyRun(RulesToExecute.Rule rule, InputManager input) {
    return rule == RulesToExecute.Rule.SegmentSizeRule && (input.getTableType().equalsIgnoreCase("OFFLINE") || input
        .getTableType().equalsIgnoreCase("HYBRID"));
  }
}
