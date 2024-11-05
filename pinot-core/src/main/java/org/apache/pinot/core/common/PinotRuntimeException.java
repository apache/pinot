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
package org.apache.pinot.core.common;

/**
 *  Simple runtime query exception containing useful contextual information, e.g. table or column name.
 */
public class PinotRuntimeException extends RuntimeException {

  //table name or alias
  private String _tableName;

  // column or expression name
  private String _columnName;

  private PinotRuntimeException(Throwable e) {
    super(e);
  }

  public static PinotRuntimeException create(Throwable e) {
    if (e instanceof PinotRuntimeException) {
      return (PinotRuntimeException) e;
    } else {
      return new PinotRuntimeException(e);
    }
  }

  public PinotRuntimeException withColumnName(String columnName) {
    _columnName = columnName;
    return this;
  }

  public PinotRuntimeException withTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  @Override
  public String getMessage() {
    StringBuilder message = new StringBuilder("Error when processing");
    if (_tableName != null) {
      message.append(" ").append(_tableName);
    }
    if (_columnName != null) {
      if (_tableName != null) {
        message.append(".");
      } else {
        message.append(" ");
      }
      message.append(_columnName);
    }
    message.append(": ");
    message.append(super.getMessage());

    return message.toString();
  }
}
