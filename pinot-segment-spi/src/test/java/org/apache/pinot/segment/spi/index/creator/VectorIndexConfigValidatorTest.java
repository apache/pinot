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
package org.apache.pinot.segment.spi.index.creator;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for {@link VectorIndexConfigValidator}.
 *
 * <p>Covers validation of HNSW and IVF_FLAT configs, cross-backend property rejection,
 * default values, and backward compatibility scenarios.</p>
 */
public class VectorIndexConfigValidatorTest {

  // ============================================================
  // Backward compatibility tests
  // ============================================================

  @Test
  public void testBackwardCompatConfigWithoutVectorIndexTypeDefaultsToHnsw() {
    // Existing configs may not have vectorIndexType at all. They must default to HNSW.
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorDimension", "1536");
    properties.put("vectorDistanceFunction", "COSINE");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorBackendType backendType = VectorIndexConfigValidator.resolveBackendType(config);
    org.testng.Assert.assertNull(config.getVectorIndexType());
    assertEquals(backendType, VectorBackendType.HNSW);
  }

  @Test
  public void testBackwardCompatEmptyVectorIndexTypeDefaultsToHnsw() {
    VectorIndexConfig config = new VectorIndexConfig(false, "", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(VectorIndexConfigValidator.resolveBackendType(config), VectorBackendType.HNSW);
  }

  @Test
  public void testExistingHnswConfigStillValidates() {
    // This is the exact format used by existing tests and configs
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "1536");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("version", "1");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    // Must not throw
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Valid HNSW config tests
  // ============================================================

  @Test
  public void testValidHnswConfigExplicitType() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("maxCon", "32");
    properties.put("beamWidth", "200");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
    assertEquals(config.resolveBackendType(), VectorBackendType.HNSW);
  }

  @Test
  public void testValidHnswConfigWithAllProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "512");
    properties.put("vectorDistanceFunction", "INNER_PRODUCT");
    properties.put("maxCon", "16");
    properties.put("beamWidth", "100");
    properties.put("maxDimensions", "4096");
    properties.put("maxBufferSizeMB", "256.5");
    properties.put("useCompoundFile", "true");
    properties.put("mode", "BEST_SPEED");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testValidHnswConfigMinimal() {
    // Minimal config: only required fields
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Valid IVF_FLAT config tests
  // ============================================================

  @Test
  public void testValidIvfFlatConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("trainSampleSize", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_FLAT);
  }

  @Test
  public void testValidIvfFlatConfigWithAllProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "1024");
    properties.put("vectorDistanceFunction", "L2");
    properties.put("nlist", "256");
    properties.put("trainSampleSize", "50000");
    properties.put("trainingSeed", "42");
    properties.put("minRowsForIndex", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testValidIvfFlatConfigMinimal() {
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_FLAT", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testValidIvfFlatConfigWithSq8Quantizer() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "384");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "64");
    properties.put("trainSampleSize", "2048");
    properties.put("quantizer", "SQ8");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_FLAT);
  }

  @Test
  public void testValidIvfOnDiskConfigWithSq4Quantizer() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_ON_DISK");
    properties.put("vectorDimension", "256");
    properties.put("vectorDistanceFunction", "EUCLIDEAN");
    properties.put("nlist", "32");
    properties.put("trainSampleSize", "1024");
    properties.put("minRowsForIndex", "500");
    properties.put("quantizer", "SQ4");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_ON_DISK);
  }

  @Test
  public void testValidIvfPqConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("pqM", "32");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_PQ);
  }

  @Test
  public void testValidIvfPqConfigWithTrainingSeed() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "512");
    properties.put("vectorDistanceFunction", "INNER_PRODUCT");
    properties.put("nlist", "64");
    properties.put("pqM", "16");
    properties.put("pqNbits", "6");
    properties.put("trainSampleSize", "4096");
    properties.put("trainingSeed", "42");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*IVF_PQ property 'pqM' is required.*")
  public void testRejectIvfPqConfigMissingRequiredProperty() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Cross-backend property rejection tests
  // ============================================================

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*nlist.*cannot be used with vectorIndexType HNSW.*")
  public void testRejectIvfFlatPropertiesOnHnswConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");  // IVF_FLAT-only property

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*trainSampleSize.*cannot be used with vectorIndexType HNSW.*")
  public void testRejectTrainSampleSizeOnHnswConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("trainSampleSize", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*maxCon.*cannot be used with vectorIndexType IVF_FLAT.*")
  public void testRejectHnswPropertiesOnIvfFlatConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("maxCon", "16");  // HNSW-only property

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*beamWidth.*cannot be used with vectorIndexType IVF_FLAT.*")
  public void testRejectBeamWidthOnIvfFlatConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("beamWidth", "200");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*HNSW supports only quantizer='FLAT'.*")
  public void testRejectSq8QuantizerOnHnswConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("quantizer", "SQ8");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*IVF_FLAT does not support quantizer='PQ'.*")
  public void testRejectPqQuantizerOnIvfFlatConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "64");
    properties.put("trainSampleSize", "1024");
    properties.put("quantizer", "PQ");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testIvfPqAcceptsLegacyFlatQuantizer() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("pqM", "32");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "10000");
    properties.put("quantizer", "FLAT");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_PQ);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*IVF_PQ supports quantizer='PQ'.*")
  public void testRejectSq4QuantizerOnIvfPqConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("pqM", "32");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "10000");
    properties.put("quantizer", "SQ4");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*pqM.*cannot be used with vectorIndexType HNSW.*")
  public void testRejectIvfPqPropertyOnHnswConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("pqM", "16");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Missing required field tests
  // ============================================================

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*vectorDimension must be a positive integer.*")
  public void testRejectZeroDimension() {
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 0, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*vectorDimension must be a positive integer.*")
  public void testRejectNegativeDimension() {
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", -5, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*vectorDistanceFunction is required.*")
  public void testRejectNullDistanceFunction() {
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 768, 1, null, null);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Property value validation tests
  // ============================================================

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*HNSW maxCon must be a positive integer.*")
  public void testRejectNegativeMaxCon() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("maxCon", "-1");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*HNSW maxCon must be a valid integer.*")
  public void testRejectNonNumericMaxCon() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("maxCon", "notANumber");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*IVF_FLAT nlist must be a positive integer.*")
  public void testRejectZeroNlist() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "0");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*trainSampleSize.*must be >= nlist.*")
  public void testRejectTrainSampleSizeLessThanNlist() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "256");
    properties.put("trainSampleSize", "100");  // less than nlist

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testTrainSampleSizeEqualToNlistIsValid() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("trainSampleSize", "128");  // equal to nlist

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*pqM.*divide vectorDimension.*")
  public void testRejectIvfPqPqMThatDoesNotDivideDimension() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "770");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("pqM", "32");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*pqNbits must be one of \\[4, 6, 8\\].*")
  public void testRejectIvfPqUnsupportedPqNbits() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("pqM", "32");
    properties.put("pqNbits", "7");
    properties.put("trainSampleSize", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*IVF_PQ trainSampleSize \\(100\\) must be >= nlist \\(256\\).*")
  public void testRejectIvfPqTrainSampleSizeLessThanNlist() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "256");
    properties.put("pqM", "32");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "100");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Distance function tests
  // ============================================================

  @Test
  public void testAllDistanceFunctions() {
    for (VectorIndexConfig.VectorDistanceFunction df : VectorIndexConfig.VectorDistanceFunction.values()) {
      VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 768, 1, df, null);
      VectorIndexConfigValidator.validate(config);
    }
  }

  @Test
  public void testL2DistanceFunctionForIvfFlat() {
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_FLAT", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.L2, null);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testL2DistanceFunctionForIvfPq() {
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_PQ", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.L2,
        Map.of("nlist", "128", "pqM", "32", "pqNbits", "8", "trainSampleSize", "10000"));
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Disabled config tests
  // ============================================================

  @Test
  public void testDisabledConfigSkipsValidation() {
    // A disabled config should not be validated even if it has invalid fields
    VectorIndexConfig config = new VectorIndexConfig(true);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Backend type resolution tests
  // ============================================================

  @Test
  public void testResolveBackendTypeExplicitHnsw() {
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(config.resolveBackendType(), VectorBackendType.HNSW);
  }

  @Test
  public void testResolveBackendTypeExplicitIvfFlat() {
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_FLAT", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_FLAT);
  }

  @Test
  public void testResolveBackendTypeExplicitIvfPq() {
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_PQ", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_PQ);
  }

  @Test
  public void testResolveBackendTypeCaseInsensitive() {
    VectorIndexConfig config = new VectorIndexConfig(false, "hnsw", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(config.resolveBackendType(), VectorBackendType.HNSW);

    VectorIndexConfig config2 = new VectorIndexConfig(false, "ivf_flat", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(config2.resolveBackendType(), VectorBackendType.IVF_FLAT);

    VectorIndexConfig config3 = new VectorIndexConfig(false, "ivf_pq", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(config3.resolveBackendType(), VectorBackendType.IVF_PQ);
  }

  @Test
  public void testResolveBackendTypeNullDefaultsToHnsw() {
    VectorIndexConfig config = new VectorIndexConfig(false, null, 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    assertEquals(config.resolveBackendType(), VectorBackendType.HNSW);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Unknown vector backend type.*FAISS.*")
  public void testResolveBackendTypeUnknown() {
    VectorIndexConfig config = new VectorIndexConfig(false, "FAISS", 768, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, null);
    config.resolveBackendType();
  }

  // ============================================================
  // Config from properties map (legacy format) tests
  // ============================================================

  @Test
  public void testLegacyPropertiesMapHnsw() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "1536");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("version", "1");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    assertNotNull(config);
    assertEquals(config.getVectorIndexType(), "HNSW");
    assertEquals(config.getVectorDimension(), 1536);
    assertEquals(config.getVectorDistanceFunction(), VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertEquals(config.getVersion(), 1);
    assertEquals(config.resolveBackendType(), VectorBackendType.HNSW);

    // Validation should pass
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testLegacyPropertiesMapIvfFlat() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "256");
    properties.put("trainSampleSize", "50000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_FLAT);

    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testLegacyPropertiesMapIvfPq() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "768");
    properties.put("vectorDistanceFunction", "COSINE");
    properties.put("nlist", "128");
    properties.put("pqM", "32");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "10000");

    VectorIndexConfig config = new VectorIndexConfig(properties);
    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_PQ);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // JSON deserialization backward compatibility
  // ============================================================

  @Test
  public void testJsonConfigWithoutVectorIndexType()
      throws Exception {
    // Simulate JSON config without vectorIndexType field (old format)
    String confStr = "{"
        + "\"disabled\": false,"
        + "\"vectorDimension\": 768,"
        + "\"version\": 1,"
        + "\"vectorDistanceFunction\": \"COSINE\""
        + "}";
    VectorIndexConfig config =
        org.apache.pinot.spi.utils.JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    // vectorIndexType should be null, resolveBackendType should default to HNSW
    assertEquals(config.resolveBackendType(), VectorBackendType.HNSW);

    // Validation should pass
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testJsonConfigWithExplicitIvfFlat()
      throws Exception {
    String confStr = "{"
        + "\"disabled\": false,"
        + "\"vectorIndexType\": \"IVF_FLAT\","
        + "\"vectorDimension\": 768,"
        + "\"version\": 1,"
        + "\"vectorDistanceFunction\": \"COSINE\","
        + "\"properties\": {"
        + "  \"nlist\": \"128\","
        + "  \"trainSampleSize\": \"10000\""
        + "}"
        + "}";
    VectorIndexConfig config =
        org.apache.pinot.spi.utils.JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_FLAT);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testJsonConfigWithExplicitIvfPq()
      throws Exception {
    String confStr = "{"
        + "\"disabled\": false,"
        + "\"vectorIndexType\": \"IVF_PQ\","
        + "\"vectorDimension\": 768,"
        + "\"version\": 1,"
        + "\"vectorDistanceFunction\": \"COSINE\","
        + "\"properties\": {"
        + "  \"nlist\": \"128\","
        + "  \"pqM\": \"32\","
        + "  \"pqNbits\": \"8\","
        + "  \"trainSampleSize\": \"10000\""
        + "}"
        + "}";
    VectorIndexConfig config =
        org.apache.pinot.spi.utils.JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    assertEquals(config.resolveBackendType(), VectorBackendType.IVF_PQ);
    VectorIndexConfigValidator.validate(config);
  }

  // ============================================================
  // Recommended defaults helper tests (Item 4)
  // ============================================================

  @Test
  public void testRecommendedIvfPqDefaultsForDim128() {
    Map<String, String> defaults = VectorIndexConfigValidator.recommendedIvfPqDefaults(128, "EUCLIDEAN");
    assertEquals(defaults.get("vectorIndexType"), "IVF_PQ");
    assertEquals(defaults.get("vectorDimension"), "128");
    assertEquals(defaults.get("vectorDistanceFunction"), "EUCLIDEAN");
    assertEquals(defaults.get("nlist"), "128");
    assertEquals(defaults.get("pqNbits"), "8");

    int pqM = Integer.parseInt(defaults.get("pqM"));
    assertNotNull(defaults.get("pqM"));
    assert pqM > 0 : "pqM must be positive";
    assert 128 % pqM == 0 : "pqM must evenly divide dimension";

    int trainSampleSize = Integer.parseInt(defaults.get("trainSampleSize"));
    int nlist = Integer.parseInt(defaults.get("nlist"));
    assert trainSampleSize >= nlist : "trainSampleSize must be >= nlist";

    // Verify the defaults pass validation
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_PQ", 128, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, defaults);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testRecommendedIvfPqDefaultsForDim512() {
    Map<String, String> defaults = VectorIndexConfigValidator.recommendedIvfPqDefaults(512, "COSINE");
    int pqM = Integer.parseInt(defaults.get("pqM"));
    assert 512 % pqM == 0 : "pqM must evenly divide dimension=512";
    assert pqM <= 64 : "pqM should be <= dim/8 for dim=512";

    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_PQ", 512, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, defaults);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testRecommendedIvfPqDefaultsForOddDimension() {
    // dim=7 is prime, only pqM=1 or pqM=7 divide evenly
    Map<String, String> defaults = VectorIndexConfigValidator.recommendedIvfPqDefaults(7, "EUCLIDEAN");
    int pqM = Integer.parseInt(defaults.get("pqM"));
    assert 7 % pqM == 0 : "pqM must evenly divide dimension=7";
    assertEquals(pqM, 1, "For prime dimension=7, pqM should fall back to 1");

    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_PQ", 7, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, defaults);
    VectorIndexConfigValidator.validate(config);
  }

  @Test
  public void testFindBestPqM() {
    assertEquals(VectorIndexConfigValidator.findBestPqM(128), 16);  // 128/8 = 16, divides evenly
    assertEquals(VectorIndexConfigValidator.findBestPqM(512), 64);  // 512/8 = 64, divides evenly
    assertEquals(VectorIndexConfigValidator.findBestPqM(7), 1);     // prime, only 1 or 7 work, target=0 -> 1
    assertEquals(VectorIndexConfigValidator.findBestPqM(1), 1);     // edge case
    assertEquals(VectorIndexConfigValidator.findBestPqM(100), 10);  // 100/8=12, nearest divisor is 10
  }
}
