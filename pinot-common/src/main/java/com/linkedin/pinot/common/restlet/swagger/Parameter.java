/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.restlet.swagger;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * Documentation for a method parameter.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Parameter {
  /** The name of the parameter (can be named differently in the exposed API than in the Java code). */
  String name();
  /** Whether the argument is a "path" parameter or a "query" parameter. */
  String in();
  /** The description of the parameter */
  String description();
  /** Whether or not the parameter is required */
  boolean required() default false;
}
