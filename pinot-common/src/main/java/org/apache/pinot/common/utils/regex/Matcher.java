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
package org.apache.pinot.common.utils.regex;

/**
 * Interface for different regex library Matcher implementations. Since no regex library is optimal in all cases,
 * this is used to allow regex libraries to be configured. This class maintains the same semantics to java.util.regex
 *
 * Matcher should be created via @link{Pattern.matcher(CharSequence input)}
 */
public interface Matcher {
  boolean matches();

  Matcher reset(CharSequence input);

  boolean find();

  int groupCount();

  String group(int group);
}
