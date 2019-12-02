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
package org.apache.pinot.spi.annotations;

import java.lang.annotation.Documented;


/**
 * Annotation to inform users of how much to rely on a particular package,
 * class or method not changing over time. Currently the stability can be
 * {@link InterfaceStability.Stable},
 * {@link InterfaceStability.Evolving} or
 * {@link InterfaceStability.Unstable}. <br>
 *
 * <ul>
 *   <li>All classes that are annotated with {@link InterfaceAudience.Public} must have
 *       InterfaceStability annotation. </li>
 *   <li>Classes that are {@link InterfaceAudience.Private} are to be considered unstable
 *       unless a different InterfaceStability annotation states otherwise.</li>
 *   <li>Pinot contributors should NOT make incompatible changes to classes marked as stable.
 *       Some things to watch out for classes marked as stable:
 *       <ul>
 *         <li> Method(s) cannot be removed from classes/interfaces marked as stable during minor version releases.
 *              Deprecate the method(s) first and remove the method in a major release.</li>
 *         <li> Similar to earlier point, method signature cannot change.</li>
 *         <li> New methods cannot be added to interfaces without providing default implementation for minor version
 *              releases.</li></ul>
 *       </ul>
 *    </li>
 * </ul>
 *
 * Note: the definitions are borrowed from a similar annotation from Apache Hadoop project.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InterfaceStability {
  /**
   * Can evolve while retaining compatibility for minor release boundaries.;
   * can break compatibility only at major release (ie. at m.0).
   */
  @Documented
  public @interface Stable {
  }

  /**
   * Evolving, but can break compatibility at minor release (i.e. m.x)
   */
  @Documented
  public @interface Evolving {
  }

  /**
   * No guarantee is provided as to reliability or stability across any
   * level of release granularity.
   */
  @Documented
  public @interface Unstable {
  }
}
