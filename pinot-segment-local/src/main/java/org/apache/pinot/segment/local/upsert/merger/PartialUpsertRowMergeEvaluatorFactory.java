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
package org.apache.pinot.segment.local.upsert.merger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartialUpsertRowMergeEvaluatorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartialUpsertRowMergeEvaluatorFactory.class);

    private PartialUpsertRowMergeEvaluatorFactory() {
    }

    public static PartialUpsertRowMergeEvaluator getInstance(String implementationClassName) {
        LOGGER.info("Creating PartialUpsertRowMergeEvaluator for class {}", implementationClassName);
        if (StringUtils.isBlank(implementationClassName)) {
            throw new IllegalArgumentException("Empty implementationClassName");
        }
        try {
            Class<?> aClass = Class.forName(implementationClassName);
            if (!PartialUpsertRowMergeEvaluator.class.isAssignableFrom(aClass)) {
                throw new IllegalArgumentException(
                        "The provided class is not an implementation of PartialUpsertRowMergeEvaluator");
            }
            return (PartialUpsertRowMergeEvaluator) aClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create a PartialUpsertRowMergeEvaluator with class %s",
                implementationClassName), e);
        }
    }
}
