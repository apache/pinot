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
package org.apache.pinot.core.function.scalar;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the `inIdSet` scalar function on its own; `IdSet.json` in pinot-query-runtime runs it in queries.
public class IdSetFunctionsTest {

  @Test
  public void testLooksUpEveryValueType()
      throws IOException {
    IdSet intIdSet = IdSets.create(DataType.INT);
    intIdSet.add(1);
    assertInIdSet(intIdSet, 1, 2);
    // BOOLEAN values are looked up as their stored INT values
    assertInIdSet(intIdSet, true, false);

    IdSet longIdSet = IdSets.create(DataType.LONG);
    longIdSet.add(10_000_000_000L);
    assertInIdSet(longIdSet, 10_000_000_000L, 1L);
    // TIMESTAMP values are looked up as their stored LONG values
    assertInIdSet(longIdSet, new Timestamp(10_000_000_000L), new Timestamp(1L));

    IdSet floatIdSet = createBloomFilterIdSet(DataType.FLOAT);
    floatIdSet.add(1.5f);
    assertInIdSet(floatIdSet, 1.5f, 2.5f);

    IdSet doubleIdSet = createBloomFilterIdSet(DataType.DOUBLE);
    doubleIdSet.add(1.25);
    assertInIdSet(doubleIdSet, 1.25, 2.25);

    IdSet stringIdSet = createBloomFilterIdSet(DataType.STRING);
    stringIdSet.add("foo");
    assertInIdSet(stringIdSet, "foo", "bar");

    IdSet bytesIdSet = createBloomFilterIdSet(DataType.BYTES);
    bytesIdSet.add(new byte[]{1, 2});
    assertInIdSet(bytesIdSet, new byte[]{1, 2}, new byte[]{3});
    // Single-stage post-aggregation passes BYTES group keys as ByteArray
    assertInIdSet(bytesIdSet, new ByteArray(new byte[]{1, 2}), new ByteArray(new byte[]{3}));

    // UUID values are looked up as their stored BYTES values
    UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    IdSet uuidIdSet = createBloomFilterIdSet(DataType.BYTES);
    uuidIdSet.add(UuidUtils.toBytes(uuid));
    assertInIdSet(uuidIdSet, uuid, UUID.fromString("123e4567-e89b-12d3-a456-426614174001"));
  }

  private static IdSet createBloomFilterIdSet(DataType dataType) {
    // Sized so that a single id cannot produce a false positive
    return IdSets.create(dataType, 0, 100, 0.0001);
  }

  private static void assertInIdSet(IdSet idSet, Object member, Object nonMember)
      throws IOException {
    IdSetFunctions idSetFunctions = new IdSetFunctions();
    String serializedIdSet = idSet.toBase64String();
    assertEquals(idSetFunctions.inIdSet(member, serializedIdSet), Boolean.TRUE);
    assertEquals(idSetFunctions.inIdSet(nonMember, serializedIdSet), Boolean.FALSE);
  }

  @Test
  public void testNullArguments()
      throws IOException {
    IdSet intIdSet = IdSets.create(DataType.INT);
    intIdSet.add(1);
    IdSetFunctions idSetFunctions = new IdSetFunctions();
    // Whether NULL is in a non-empty IdSet is unknown, as for an IN subquery
    assertNull(idSetFunctions.inIdSet(null, intIdSet.toBase64String()));
    // A null IdSet, which IDSET returns over zero rows with null handling enabled, holds no ids, so no value is in it,
    // not even NULL
    assertEquals(idSetFunctions.inIdSet(1, null), Boolean.FALSE);
    assertEquals(idSetFunctions.inIdSet(null, null), Boolean.FALSE);
    // The same holds for an empty IdSet, which IDSET returns over zero rows with null handling disabled
    assertEquals(idSetFunctions.inIdSet(null, IdSets.emptyIdSet().toBase64String()), Boolean.FALSE);
  }

  @Test
  public void testEmptyIdSetAcceptsEveryValueType()
      throws IOException {
    String serializedEmptyIdSet = IdSets.emptyIdSet().toBase64String();
    IdSetFunctions idSetFunctions = new IdSetFunctions();
    for (Object value : List.of(1, 1L, 1.5f, 1.25, "foo", new byte[]{1}, true, new Timestamp(1L))) {
      assertEquals(idSetFunctions.inIdSet(value, serializedEmptyIdSet), Boolean.FALSE);
    }
  }

  @Test
  public void testRejectsUnsupportedValueTypes()
      throws IOException {
    IdSet intIdSet = IdSets.create(DataType.INT);
    intIdSet.add(1);
    String serializedIntIdSet = intIdSet.toBase64String();
    IdSetFunctions idSetFunctions = new IdSetFunctions();
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> idSetFunctions.inIdSet(1L, serializedIntIdSet));
    assertEquals(exception.getMessage(), "Cannot look up LONG values in an IdSet built from INT values");
    exception = expectThrows(IllegalArgumentException.class,
        () -> idSetFunctions.inIdSet(new BigDecimal("1"), serializedIntIdSet));
    assertTrue(exception.getMessage().startsWith("Cannot look up BIG_DECIMAL values in an IdSet"),
        exception.getMessage());
    exception =
        expectThrows(IllegalArgumentException.class, () -> idSetFunctions.inIdSet(new int[]{1}, serializedIntIdSet));
    assertTrue(exception.getMessage().startsWith("Cannot look up values of class"), exception.getMessage());

    // Each value class is validated against the IdSet it is looked up in, also after another class passed
    assertEquals(idSetFunctions.inIdSet(1, serializedIntIdSet), Boolean.TRUE);
    expectThrows(IllegalArgumentException.class, () -> idSetFunctions.inIdSet(1L, serializedIntIdSet));
    IdSet longIdSet = IdSets.create(DataType.LONG);
    longIdSet.add(1L);
    assertEquals(idSetFunctions.inIdSet(1L, longIdSet.toBase64String()), Boolean.TRUE);
    expectThrows(IllegalArgumentException.class, () -> idSetFunctions.inIdSet(1, longIdSet.toBase64String()));
  }

  @Test
  public void testFollowsTheIdSetOfEachCall()
      throws IOException {
    IdSet idSet1 = IdSets.create(DataType.INT);
    idSet1.add(1);
    IdSet idSet2 = IdSets.create(DataType.INT);
    idSet2.add(2);
    String serializedIdSet1 = idSet1.toBase64String();
    String serializedIdSet2 = idSet2.toBase64String();
    // One instance serves one call site, whose IdSet can change from row to row when it is read from a column
    IdSetFunctions idSetFunctions = new IdSetFunctions();
    assertEquals(idSetFunctions.inIdSet(1, serializedIdSet1), Boolean.TRUE);
    assertEquals(idSetFunctions.inIdSet(2, serializedIdSet1), Boolean.FALSE);
    assertEquals(idSetFunctions.inIdSet(1, serializedIdSet2), Boolean.FALSE);
    assertEquals(idSetFunctions.inIdSet(2, serializedIdSet2), Boolean.TRUE);
    // An equal but distinct String instance holds the same IdSet
    assertEquals(idSetFunctions.inIdSet(2, new String(serializedIdSet2)), Boolean.TRUE);
    assertEquals(idSetFunctions.inIdSet(1, new String(serializedIdSet1)), Boolean.TRUE);
  }

  @Test
  public void testReusesTheDeserializedIdSet()
      throws IOException {
    IdSet idSet1 = IdSets.create(DataType.INT);
    idSet1.add(1);
    String serializedIdSet1 = idSet1.toBase64String();
    IdSetFunctions idSetFunctions = new IdSetFunctions();
    IdSet deserializedIdSet = idSetFunctions.getIdSet(serializedIdSet1);
    assertSame(idSetFunctions.getIdSet(serializedIdSet1), deserializedIdSet);
    // Rows of a new data block carry a new but equal String instance
    assertSame(idSetFunctions.getIdSet(new String(serializedIdSet1)), deserializedIdSet);
    // A different IdSet is deserialized
    IdSet idSet2 = IdSets.create(DataType.INT);
    idSet2.add(2);
    assertNotSame(idSetFunctions.getIdSet(idSet2.toBase64String()), deserializedIdSet);
  }

  @Test
  public void testRejectsMalformedIdSet() {
    IdSetFunctions idSetFunctions = new IdSetFunctions();
    // Not Base64
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> idSetFunctions.inIdSet(1, "not an IdSet"));
    assertTrue(exception.getMessage().startsWith("Caught exception while deserializing IdSet"), exception.getMessage());
    // Valid Base64 for an unknown IdSet type id
    exception = expectThrows(IllegalArgumentException.class, () -> idSetFunctions.inIdSet(1, "CQ=="));
    assertTrue(exception.getMessage().contains("Unsupported IdSet type id: 9"), exception.getMessage());
  }

  @Test
  public void testRegisteredAsScalarFunction()
      throws IOException {
    FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo("inidset", 2);
    assertNotNull(functionInfo);
    assertEquals(functionInfo.getMethod().getDeclaringClass(), IdSetFunctions.class);
    // Kept out of compile-time evaluation, which passes literals as types the lookup cannot use
    assertFalse(functionInfo.isDeterministic());
    // The parameters are nullable, so the invoker passes null arguments through instead of returning null
    FunctionInvoker functionInvoker = new FunctionInvoker(functionInfo);
    assertEquals(functionInvoker.invoke(new Object[]{1, null}), Boolean.FALSE);
    IdSet intIdSet = IdSets.create(DataType.INT);
    intIdSet.add(1);
    assertNull(functionInvoker.invoke(new Object[]{null, intIdSet.toBase64String()}));
  }
}
