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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BytesUtils;


public class LiteralHintUtils {
  private LiteralHintUtils() {
  }

  public static String literalMapToHintString(Map<Pair<Integer, Integer>, RexExpression.Literal> literals) {
    List<String> literalStrings = new ArrayList<>(literals.size());
    for (Map.Entry<Pair<Integer, Integer>, RexExpression.Literal> e : literals.entrySet()) {
      // individual literal parts are joined with `|`
      literalStrings.add(
          String.format("%d|%d|%s|%s", e.getKey().left, e.getKey().right, e.getValue().getDataType().name(),
              e.getValue().getValue()));
    }
    // semi-colon is used to separate between encoded literals
    return "{" + StringUtils.join(literalStrings, ";:;") + "}";
  }

  public static Map<Integer, Map<Integer, Literal>> hintStringToLiteralMap(String literalString) {
    Map<Integer, Map<Integer, Literal>> aggCallToLiteralArgsMap = new HashMap<>();
    if (StringUtils.isNotEmpty(literalString) && !"{}".equals(literalString)) {
      String[] literalStringArr = literalString.substring(1, literalString.length() - 1).split(";:;");
      for (String literalStr : literalStringArr) {
        String[] literalStrParts = literalStr.split("\\|", 4);
        int aggIdx = Integer.parseInt(literalStrParts[0]);
        int argListIdx = Integer.parseInt(literalStrParts[1]);
        String dataTypeNameStr = literalStrParts[2];
        String valueStr = literalStrParts[3];
        Map<Integer, Literal> literalArgs = aggCallToLiteralArgsMap.computeIfAbsent(aggIdx, i -> new HashMap<>());
        literalArgs.put(argListIdx, stringToLiteral(dataTypeNameStr, valueStr));
      }
    }
    return aggCallToLiteralArgsMap;
  }

  private static Literal stringToLiteral(String dataTypeStr, String valueStr) {
    FieldSpec.DataType dataType = FieldSpec.DataType.valueOf(dataTypeStr);
    switch (dataType) {
      case BOOLEAN:
        return Literal.boolValue(valueStr.equals("1"));
      case INT:
        return Literal.intValue(Integer.parseInt(valueStr));
      case LONG:
        return Literal.longValue(Long.parseLong(valueStr));
      case FLOAT:
      case DOUBLE:
        return Literal.doubleValue(Double.parseDouble(valueStr));
      case STRING:
        return Literal.stringValue(valueStr);
      case BYTES:
        return Literal.binaryValue(BytesUtils.toBytes(valueStr));
      default:
        throw new UnsupportedOperationException("Unsupported RexLiteral type: " + dataTypeStr);
    }
  }
}
