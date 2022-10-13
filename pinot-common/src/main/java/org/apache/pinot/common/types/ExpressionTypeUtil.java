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
package org.apache.pinot.common.types;

import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class ExpressionTypeUtil {

  private ExpressionTypeUtil() {
    // utility class
  }

  public static PinotDataType getTypeForLiteral(Literal literal) {
    if (literal.isSetBoolValue()) {
      return PinotDataType.BOOLEAN;
    } else if (literal.isSetBinaryValue()) {
      return PinotDataType.BYTES;
    } else if (literal.isSetByteValue()) {
      return PinotDataType.BYTE;
    } else if (literal.isSetIntValue()) {
      return PinotDataType.INTEGER;
    } else if (literal.isSetLongValue()) {
      return PinotDataType.LONG;
    } else if (literal.isSetDoubleValue()) {
      return PinotDataType.DOUBLE;
    } else if (literal.isSetStringValue()) {
      return PinotDataType.STRING;
    } else {
      throw new IllegalArgumentException("Unexpected literal " + literal);
    }
  }

  public static PinotDataType getType(Expression expression, Schema schema) {
    switch (expression.getType()) {
      case LITERAL:
        // all literals are single-valued at this point
        return getTypeForLiteral(expression.getLiteral());
      case IDENTIFIER:
        FieldSpec identifier = schema.getFieldSpecFor(expression.getIdentifier().getName());
        if (identifier == null) {
          throw new IllegalArgumentException("Unknown identifier " + expression.getIdentifier());
        }
        return PinotDataType.getPinotDataTypeForIngestion(identifier);
      case FUNCTION:
        // for now, all nested function calls just return OBJECT, which doesn't give us
        // type safety but will help maintain backwards compatibility
        return PinotDataType.OBJECT;
      default:
        throw new IllegalArgumentException("Unexpected expression type: " + expression.getType());
    }
  }

  public static PinotDataType getType(ExpressionContext expressionContext, Map<String, FieldSpec> schema) {
    return getType(expressionContext, identifier -> {
      FieldSpec spec = schema.get(identifier);
      if (spec == null) {
        throw new IllegalArgumentException("Unknown identifier " + expressionContext.getIdentifier());
      }
      return spec;
    });
  }

  public static PinotDataType getTypeDS(ExpressionContext expressionContext, Map<String, DataSource> schema) {
    return getType(expressionContext, identifier -> {
      DataSource dataSource = schema.get(identifier);
      if (dataSource == null) {
        throw new IllegalArgumentException("Unknown identifier " + expressionContext.getIdentifier());
      }
      return dataSource.getDataSourceMetadata().getFieldSpec();
    });
  }

  private static PinotDataType getType(ExpressionContext expressionContext, Function<String, FieldSpec> schema) {
    switch (expressionContext.getType()) {
      case LITERAL:
        // all literals are single-valued at this point
        return PinotDataType.getPinotDataTypeForIngestion(
            new DimensionFieldSpec("ignored", expressionContext.getLiteral().getType(), true));
      case IDENTIFIER:
        FieldSpec spec = schema.apply(expressionContext.getIdentifier());
        return PinotDataType.getPinotDataTypeForIngestion(spec);
      case FUNCTION:
        // for now, all nested function calls just return OBJECT, which doesn't give us
        // type safety but will help maintain backwards compatibility
        return PinotDataType.OBJECT;
      default:
        throw new IllegalArgumentException("Unexpected expression type: " + expressionContext.getType());
    }
  }
}
