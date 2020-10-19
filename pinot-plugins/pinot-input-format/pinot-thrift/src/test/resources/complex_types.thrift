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
namespace java org.apache.pinot.plugin.inputformat.thrift

struct ComplexTypes {
  1: required i32 intField;
  2: required i64 longField;
  3: required bool booleanField;
  4: required double doubleField;
  5: required string stringField;
  6: required TestEnum enumField;
  7: optional string optionalStringField;
  8: required NestedType nestedStructField;
  9: required list<string> simpleListField;
  10: required list<NestedType> complexListField;
  15: map<string,i32> simpleMapField; // Thrift IDs don't need to be consecutive. It can be any 32-bit value > 0.
  16: map<string,NestedType> complexMapField;
}

struct NestedType {
  1: required string nestedStringField;
  2: required i32 nestedIntField;
}

enum TestEnum {
  ALPHA = 1,
  BETA = 2,
  GAMMA = 3,
  DELTA = 4
}
