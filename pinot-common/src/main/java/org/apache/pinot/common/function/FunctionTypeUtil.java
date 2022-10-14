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
package org.apache.pinot.common.function;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.data.FieldSpec;


public class FunctionTypeUtil {

  private static final RelDataTypeFactory JAVA_TYPE_FACTORY = new JavaTypeFactoryImpl();

  private FunctionTypeUtil() {
  }

  public static RelDataType fromExpression(Expression expression) {
    switch (expression.getType()) {
      case LITERAL:
        return fromLiteral(expression.getLiteral());
      case IDENTIFIER:
      case FUNCTION:
        // for now, we return STRING for unknown types because we support type
        // coercion from String to any other type, which will allow us to match
        // all functions
        return JAVA_TYPE_FACTORY.createJavaType(String.class);
      default:
        throw new IllegalStateException("Unexpected expression type: " + expression.getType());
    }
  }

  public static RelDataType fromExpression(ExpressionContext expression) {
    switch (expression.getType()) {
      case LITERAL:
        return JAVA_TYPE_FACTORY.createJavaType(expression.getLiteral().getType().getJavaClass());
      case IDENTIFIER:
      case FUNCTION:
        // for now, we return STRING for unknown types because we support type
        // coercion from String to any other type, which will allow us to match
        // all functions
        return JAVA_TYPE_FACTORY.createJavaType(String.class);
      default:
        throw new IllegalStateException("Unexpected expression type: " + expression.getType());
    }
  }

  public static RelDataType fromDataType(FieldSpec.DataType dataType) {
    return JAVA_TYPE_FACTORY.createJavaType(dataType.getJavaClass());
  }

  public static RelDataType fromColumnDataType(DataSchema.ColumnDataType columnDataType) {
    return fromDataType(columnDataType.toDataType());
  }

  public static RelDataType fromLiteral(Literal literal) {
    return JAVA_TYPE_FACTORY.createJavaType(literal.getFieldValue().getClass());
  }

  public static RelDataType fromClass(Class<?> clazz) {
    return JAVA_TYPE_FACTORY.createJavaType(clazz);
  }

  public static List<RelDataType> fromClass(Class<?>... classes) {
    return Arrays.stream(classes)
        .map(FunctionTypeUtil::fromClass)
        .collect(Collectors.toList());
  }
}
