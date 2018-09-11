/**
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

package com.linkedin.thirdeye.common;

/**
 * This BaseFactory is responsible to generate an instance from a given class path.
 * @param <T> the class of return instance
 */
public class BaseFactory<T> {
  public <T> T getInstance(String classPath)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException{
    T typeInstance = (T) Class.forName(classPath).newInstance();
    return typeInstance;
  }
}
