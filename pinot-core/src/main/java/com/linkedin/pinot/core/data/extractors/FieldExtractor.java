/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


/**
 * Take a GenericRow transform it to an indexable GenericRow.
 * Customized logic will apply in transform(...)
 * Constructor of implementation should specify schema
 * Schema should not be changed for the life of the FieldExtractor
 *
 *
 */
public interface FieldExtractor {

  Schema getSchema();

  GenericRow transform(GenericRow row);

  GenericRow transform(GenericRow row, GenericRow destinationRow);
}
