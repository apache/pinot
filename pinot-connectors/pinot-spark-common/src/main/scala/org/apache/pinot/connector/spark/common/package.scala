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
package org.apache.pinot.connector.spark

import java.util.Optional

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

package object common {

  // Define a singleton ObjectMapper instance
  private val objectMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /** Parse json string to given model. */
  def decodeTo[A](jsonString: String, clazz: Class[A]): A = {
    try {
      objectMapper.readValue(jsonString, clazz)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Error occurred while parsing JSON string: ${e.getMessage}", e)
    }
  }

  def scalafyOptional[A](value: Optional[A]): Option[A] = {
    if (value.isPresent) Some(value.get()) else None
  }

}
