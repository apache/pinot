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
package org.apache.pinot.core.api;

import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import org.glassfish.hk2.api.DynamicConfigurationService;
import org.glassfish.hk2.api.MultiException;
import org.glassfish.hk2.api.Populator;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ClasspathDescriptorFileFinder;
import org.glassfish.hk2.utilities.DuplicatePostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Auto scan and discovery classes annotated with Service tags. This enables the feature similar to Spring's
 * auto-discovery, that if a class is annotated with some tags like @Service, the class will be auto-loaded
 * The code are mostly from https://mkyong.com/webservices/jax-rs/jersey-and-hk2-dependency-injection-auto-scanning/
 * <p>
 * To make a class auto-loaded, what we need to do is to add the below dependency into project:
 * <p>
 * <pre>
 *      &lt;dependency&gt;
 *         &lt;groupId&gt;org.glassfish.hk2&lt;/groupId&gt;
 *         &lt;artifactId&gt;hk2-metadata-generator&lt;/artifactId&gt;
 *         &lt;version&gt;${hk2.version}&lt;/version&gt;
 *       &lt;/dependency&gt;
 *
 * </pre>
 * <p>
 * And then annotate your class with @Service tag, like below:
 * <p>
 * <pre>{@code
 *  import org.jvnet.hk2.annotations.Service;
 *  {@literal @}Service
 *  public class WriteApiContextProvider {
 *    public SomeResult doComputation() {
 *       ...
 *    }
 *  }
 * }
 * </pre>
 * <p>
 * The class will be written into <code>META-INF/hk2-locator/default</code> path in the JAR. The ServiceAutoDiscovery
 * feature will know that this class needs to be loaded.
 *
 * <p>
 * Examples are in <code>
 * pinot-integration-tests/src/main/java/org/apache/pinot/broker/integration/tests/BrokerTestAutoLoadedService.java
 * </code>
 */
public class ServiceAutoDiscoveryFeature implements Feature {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceAutoDiscoveryFeature.class);

    @Inject
    ServiceLocator _serviceLocator;

    @Override
    public boolean configure(FeatureContext context) {
        DynamicConfigurationService dcs =
                _serviceLocator.getService(DynamicConfigurationService.class);
        Populator populator = dcs.getPopulator();
        try {
            // Populator - populate HK2 service locators from inhabitants files
            // ClasspathDescriptorFileFinder - find files from META-INF/hk2-locator/default
            populator.populate(
                    new ClasspathDescriptorFileFinder(this.getClass().getClassLoader()),
                    new DuplicatePostProcessor());
        } catch (IOException | MultiException ex) {
            LOGGER.error("Failed to register service locator. Auto-discovery will fail, but app will continue", ex);
        }
        return true;
    }
}
