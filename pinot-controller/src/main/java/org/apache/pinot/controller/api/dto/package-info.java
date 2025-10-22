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
/**
 * Data Transfer Objects (DTOs) for Pinot REST APIs.
 *
 * <p>
 * DTOs are Plain Old Java Objects (POJOs) that serve as the contract between Pinot Controller's
 * REST APIs and external clients. These objects are serialized to/from JSON when exchanged via
 * HTTP endpoints.
 * </p>
 *
 * <h2>Guidelines</h2>
 * <ul>
 *   <li><b>Public API Contract:</b> Classes here are part of Pinot's public API.
 *       Maintain backward compatibility or follow proper deprecation practices.</li>
 *   <li><b>Simple POJOs:</b> Keep DTOs as simple data containers without business logic.</li>
 *   <li><b>Serialization-Friendly:</b> All fields should be JSON-serializable.</li>
 *   <li><b>Fluent Setters:</b> Use fluent-style setters that return {@code this}.</li>
 *   <li><b>Documentation:</b> Document each DTO and its fields clearly.</li>
 * </ul>
 */
package org.apache.pinot.controller.api.dto;
