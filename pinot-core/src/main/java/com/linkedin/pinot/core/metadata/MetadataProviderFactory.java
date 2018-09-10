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
package com.linkedin.pinot.core.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The MetadataProviderFactory will instantiate the MetadataProvider class, which is used to extract metadata from
 * a given file during segment upload.
 */
public class MetadataProviderFactory {
    public static final Logger LOGGER = LoggerFactory.getLogger(MetadataProviderFactory.class);

    // Prevent factory from being instantiated
    private MetadataProviderFactory() {

    }

    public static MetadataProvider create(String metadataClassName) {
      String metadataProviderClassName = metadataClassName;
      try {
        LOGGER.info("Instantiating MetadataProvider class {}", metadataProviderClassName);
        MetadataProvider metadataProvider =  (MetadataProvider) Class.forName(metadataProviderClassName).newInstance();
        return metadataProvider;
      } catch (Exception e) {
        LOGGER.warn("No metadata provider class passed in, using default");
        return new DefaultMetadataProvider();
      }
    }
}
