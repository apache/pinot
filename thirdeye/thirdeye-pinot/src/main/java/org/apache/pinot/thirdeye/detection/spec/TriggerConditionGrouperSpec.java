/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.detection.spec;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TriggerConditionGrouperSpec extends AbstractSpec {
  private String expression;
  private String operator;
  private Map<String, Object> leftOp;
  private Map<String, Object> rightOp;

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }

  public Map<String, Object> getLeftOp() {
    return leftOp;
  }

  public void setLeftOp(Map<String, Object> leftOp) {
    this.leftOp = leftOp;
  }

  public Map<String, Object> getRightOp() {
    return rightOp;
  }

  public void setRightOp(Map<String, Object> rightOp) {
    this.rightOp = rightOp;
  }
}

