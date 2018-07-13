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
package com.linkedin.pinot.common.config;

import io.vavr.collection.Map;


/**
 * Transforms the child keys of a given configuration path into a new set of child keys. This can be used to implement
 * complex DSLs that depend on combination of keys, as well as other operations like splitting, merging, and filtering
 * of configuration keys.
 */
public interface ChildKeyTransformer {
  Map<String, ?> apply(Map<String, ?> childKeys, String pathPrefix);
  Map<String, ?> unapply(Map<String, ?> childKeys, String pathPrefix);
}
