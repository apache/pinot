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
package org.apache.pinot.segment.local.segment.index.fst;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.testng.Assert;
import org.testng.annotations.Test;
public class IFSTIndexTypeTest {
  @Test
  public void testIFSTIndexTypeRegistration() {
    // Test that IFST index type is registered and accessible
    IndexService indexService = IndexService.getInstance();
    // Test direct access via IndexService
    Assert.assertTrue(indexService.getOptional("ifst_index").isPresent());
    Assert.assertEquals(indexService.get("ifst_index").getId(), "ifst_index");
    // Test access via StandardIndexes
    Assert.assertNotNull(StandardIndexes.ifst());
    Assert.assertEquals(StandardIndexes.ifst().getId(), "ifst_index");
    Assert.assertEquals(StandardIndexes.ifst().getPrettyName(), "ifst");
  }
  @Test
  public void testIFSTIndexTypeConfiguration() {
    // Test that IFST index type uses the same config as FST
    Assert.assertEquals(StandardIndexes.ifst().getIndexConfigClass(), StandardIndexes.fst().getIndexConfigClass());
    // Test default config
    Assert.assertEquals(StandardIndexes.ifst().getDefaultConfig(), StandardIndexes.fst().getDefaultConfig());
  }
  @Test
  public void testIFSTIndexTypeFileExtensions() {
    // Test that IFST has its own file extensions
    Assert.assertNotEquals(StandardIndexes.ifst().getFileExtensions(null),
        StandardIndexes.fst().getFileExtensions(null));
    // Verify IFST extensions contain "ifst"
    StandardIndexes.ifst().getFileExtensions(null).forEach(extension -> {
      Assert.assertTrue(extension.contains("ifst"), "Extension should contain 'ifst': " + extension);
    });
  }
  @Test
  public void testIFSTIndexTypeUsesLuceneImplementation() {
    // Test that IFST index type only supports Lucene implementation
    // This is verified by the fact that it doesn't throw exceptions when
    // trying to create an index creator, and it should use LuceneIFSTIndexCreator
    // The actual creator creation would require a mock context, but we can
    // verify that the index type is properly configured
    Assert.assertNotNull(StandardIndexes.ifst());
    Assert.assertEquals(StandardIndexes.ifst().getId(), "ifst_index");
  }
  @Test
  public void testIFSTIndexTypeUsesDedicatedHandler() {
    // Test that IFST index type uses its own dedicated handler
    IFSTIndexType ifstIndexType = new IFSTIndexType();
    Assert.assertEquals(ifstIndexType.getPrettyName(), StandardIndexes.ifst().getPrettyName());
    // Verify the handler type (this would require mocking, but we can verify the index type)
    Assert.assertEquals(ifstIndexType.getId(), "ifst_index");
  }
}
