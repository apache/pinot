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
package org.apache.pinot.annotations;

import java.lang.annotation.Documented;


/**
 * Annotation to inform users of a package, class or method's intended audience.
 *
 * Currently the audience can be {@link org.apache.pinot.annotations.InterfaceAudience.Public},
 * {@link org.apache.pinot.annotations.InterfaceAudience.LimitedPrivate} or
 * {@link org.apache.pinot.annotations.InterfaceAudience.Private}. <br>
 *
 * <ul>
 * <li>Public classes that are not marked with this annotation must be
 * considered by default as {@link org.apache.pinot.annotations.InterfaceAudience.Private}.</li>
 *
 * <li>External application developers depending on Pinot must only use classes that are marked
 * {@link org.apache.pinot.annotations.InterfaceAudience.Public}. Do not depend on classes without an
 * explicit InterfaceAudience.Public annotation as these classes
 * could be removed or change in incompatible ways.</li>
 *
 * <li> Methods may have a different annotation that it is more restrictive
 * compared to the audience classification of the class. Example: A class
 * might be {@link org.apache.pinot.annotations.InterfaceAudience.Public},
 * but a method may be {@link org.apache.pinot.annotations.InterfaceAudience.LimitedPrivate}
 * </li></ul>
 *
 * The annotation is borrowed from a similar Apache Hadoop annotation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InterfaceAudience {
  /**
   * Intended for use by any project or application.
   */
  @Documented
  public @interface Public {};

  /**
   * Intended only for the project(s) specified in the annotation.
   * No specific uses for this exist currently.
   */
  @Documented
  public @interface LimitedPrivate {
    String[] value();
  };

  /**
   * Intended for use only within Pinot itself.
   */
  @Documented
  public @interface Private {};

  private InterfaceAudience() {} // Audience can't exist on its own
}