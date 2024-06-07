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
package com.google.protobuf;

public class ProtobufInternalUtils {
  private ProtobufInternalUtils() {
  }
  /**
   * The protocol buffer compiler generates a set of accessor methods for each field defined within the message in the
   * .proto file. The method name is determined by converting the .proto names to camel case using the
   *  SchemaUtil.toCamelCase(). We need this to generate the method name to call.
   */
  public static String underScoreToCamelCase(String name, boolean capNext) {
    return SchemaUtil.toCamelCase(name, capNext);
  }
}
