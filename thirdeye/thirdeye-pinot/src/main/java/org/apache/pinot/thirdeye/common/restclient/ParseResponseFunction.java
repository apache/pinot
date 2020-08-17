/*
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

package org.apache.pinot.thirdeye.common.restclient;

import java.io.IOException;


/**
 * A functional interface to allow passing a function to the rest client that would parse the response
 *
 * @param <T>  the type parameter
 * @param <R>  the type parameter
 */
public interface ParseResponseFunction<T,R> {
  /**
   * Function that will take in a respnse and parse into an object
   *
   * @param t the t
   * @return the r
   * @throws IOException the io exception
   */
  R parse(T t) throws IOException;
}