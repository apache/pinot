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
package org.apache.pinot.spi.config.table.tuner;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation class for TableConfig tuner methods.
 *
 * Methods annotated using this interface must accept 2 parameters
 * - {@link org.apache.pinot.spi.config.table.TableConfig}
 * - {@link org.apache.pinot.spi.data.Schema}
 *
 * and return type must be {@link org.apache.pinot.spi.config.table.TableConfig}
 *
 * These methods are auto registered and invoked inside Controller API.
 *
 * NOTE:
 * 1. The annotated method must be under the package of name 'org.apache.pinot.*.tuner.*' to be auto-registered.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TableConfigTuner {
  boolean enabled() default true;

  String name() default "";
}
