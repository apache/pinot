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
package org.apache.pinot.core.query.request.context;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class JoinContext {
  private final String _leftTableName;
  private final String _rawLeftTableName;
  private final String _rightTableName;
  private final ExpressionContext _leftJoinKey;
  private final String _rightJoinKey;

  public JoinContext(String leftTableName, String rightTableName, ExpressionContext leftJoinKey, String rightJoinKey) {
    _leftTableName = leftTableName;
    _rawLeftTableName = TableNameBuilder.extractRawTableName(leftTableName);
    _rightTableName = rightTableName;
    _leftJoinKey = leftJoinKey;
    _rightJoinKey = rightJoinKey;
  }

  public String getLeftTableName() {
    return _leftTableName;
  }

  public String getRawLeftTableName() {
    return _rawLeftTableName;
  }

  public String getRightTableName() {
    return _rightTableName;
  }

  public ExpressionContext getLeftJoinKey() {
    return _leftJoinKey;
  }

  public String getRightJoinKey() {
    return _rightJoinKey;
  }

  @Override
  public String toString() {
    return "JoinContext{" + "_leftTableName='" + _leftTableName + '\'' + ", _rightTableName='" + _rightTableName + '\''
        + ", _leftJoinKey=" + _leftJoinKey + ", _rightJoinKey='" + _rightJoinKey + '\'' + '}';
  }
}
