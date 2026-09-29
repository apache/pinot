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
package org.apache.pinot.plugin.inputformat.arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


/// Tests [ArrowRecordExtractor] — see its class Javadoc for the per-type contract. Each test builds a
/// single-column [VectorSchemaRoot], roundtrips it through an Arrow IPC stream so a real
/// [ArrowStreamReader] drives [ArrowRecordExtractor#setReader], extracts row 0, and asserts the value.
public class ArrowRecordExtractorTest {

  private static final String COLUMN = "col";
  private RootAllocator _allocator;

  @BeforeMethod
  public void setUp() {
    _allocator = new RootAllocator();
  }

  @AfterMethod
  public void tearDown() {
    _allocator.close();
  }

  // === Scalars (default contract mode) ===

  @Test
  public void testBoolean() throws IOException {
    Field field = field(new ArrowType.Bool());
    assertEquals(extract(field, v -> {
      ((BitVector) v).setSafe(0, 1);
      v.setValueCount(1);
    }), true);
  }

  @Test
  public void testTinyIntWidenedToInteger() throws IOException {
    Field field = field(new ArrowType.Int(8, true));
    assertEquals(extract(field, v -> {
      ((TinyIntVector) v).setSafe(0, 42);
      v.setValueCount(1);
    }), 42);
  }

  @Test
  public void testSmallIntWidenedToInteger() throws IOException {
    Field field = field(new ArrowType.Int(16, true));
    assertEquals(extract(field, v -> {
      ((SmallIntVector) v).setSafe(0, 1234);
      v.setValueCount(1);
    }), 1234);
  }

  @Test
  public void testIntPreserved() throws IOException {
    Field field = field(new ArrowType.Int(32, true));
    assertEquals(extract(field, v -> {
      ((IntVector) v).setSafe(0, 100_000);
      v.setValueCount(1);
    }), 100_000);
  }

  @Test
  public void testBigIntPreserved() throws IOException {
    Field field = field(new ArrowType.Int(64, true));
    assertEquals(extract(field, v -> {
      ((BigIntVector) v).setSafe(0, 1_588_469_340_000L);
      v.setValueCount(1);
    }), 1_588_469_340_000L);
  }

  @Test
  public void testUInt1WidenedToInteger() throws IOException {
    Field field = field(new ArrowType.Int(8, false));
    assertEquals(extract(field, v -> {
      ((UInt1Vector) v).setSafe(0, 200);
      v.setValueCount(1);
    }), 200);
  }

  @Test
  public void testUInt2CharacterWidenedToInteger() throws IOException {
    Field field = field(new ArrowType.Int(16, false));
    Object result = extract(field, v -> {
      ((UInt2Vector) v).setSafe(0, 50_000);
      v.setValueCount(1);
    });
    assertEquals(result, 50_000);
    assertSame(result.getClass(), Integer.class);
  }

  @Test
  public void testUInt4Preserved() throws IOException {
    Field field = field(new ArrowType.Int(32, false));
    assertEquals(extract(field, v -> {
      ((UInt4Vector) v).setSafe(0, 100_000);
      v.setValueCount(1);
    }), 100_000);
  }

  @Test
  public void testUInt8Preserved() throws IOException {
    Field field = field(new ArrowType.Int(64, false));
    assertEquals(extract(field, v -> {
      ((UInt8Vector) v).setSafe(0, 1234567890L);
      v.setValueCount(1);
    }), 1234567890L);
  }

  @Test
  public void testFloat() throws IOException {
    Field field = field(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    assertEquals(extract(field, v -> {
      ((Float4Vector) v).setSafe(0, 0.5f);
      v.setValueCount(1);
    }), 0.5f);
  }

  @Test
  public void testDouble() throws IOException {
    Field field = field(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    assertEquals(extract(field, v -> {
      ((Float8Vector) v).setSafe(0, 1.5);
      v.setValueCount(1);
    }), 1.5);
  }

  @Test
  public void testDecimal() throws IOException {
    Field field = field(new ArrowType.Decimal(10, 2, 128));
    BigDecimal expected = new BigDecimal("123.45");
    assertEquals(extract(field, v -> {
      ((DecimalVector) v).setSafe(0, expected);
      v.setValueCount(1);
    }), expected);
  }

  @Test
  public void testUtf8() throws IOException {
    Field field = field(new ArrowType.Utf8());
    Object result = extract(field, v -> {
      ((VarCharVector) v).setSafe(0, "hello".getBytes());
      v.setValueCount(1);
    });
    assertEquals(result, "hello");
    assertSame(result.getClass(), String.class);
  }

  @Test
  public void testLargeUtf8() throws IOException {
    Field field = field(new ArrowType.LargeUtf8());
    Object result = extract(field, v -> {
      ((LargeVarCharVector) v).setSafe(0, "world".getBytes());
      v.setValueCount(1);
    });
    assertEquals(result, "world");
    assertSame(result.getClass(), String.class);
  }

  @Test
  public void testBinary() throws IOException {
    Field field = field(new ArrowType.Binary());
    byte[] bytes = {1, 2, 3, 4};
    Object result = extract(field, v -> {
      ((VarBinaryVector) v).setSafe(0, bytes);
      v.setValueCount(1);
    });
    assertEquals((byte[]) result, bytes);
  }

  @Test
  public void testLargeBinary() throws IOException {
    Field field = field(new ArrowType.LargeBinary());
    byte[] bytes = {5, 6, 7};
    Object result = extract(field, v -> {
      ((LargeVarBinaryVector) v).setSafe(0, bytes);
      v.setValueCount(1);
    });
    assertEquals((byte[]) result, bytes);
  }

  @Test
  public void testFixedSizeBinary() throws IOException {
    Field field = field(new ArrowType.FixedSizeBinary(4));
    byte[] bytes = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
    Object result = extract(field, v -> {
      ((FixedSizeBinaryVector) v).setSafe(0, bytes);
      v.setValueCount(1);
    });
    assertEquals((byte[]) result, bytes);
  }

  @Test
  public void testNullType() throws IOException {
    Field field = field(new ArrowType.Null());
    assertNull(extract(field, v -> {
      ((NullVector) v).setValueCount(1);
    }));
  }

  // === Temporal (default contract mode) ===

  @Test
  public void testDateDay() throws IOException {
    Field field = field(new ArrowType.Date(DateUnit.DAY));
    assertEquals(extract(field, v -> {
      ((DateDayVector) v).setSafe(0, 19_000); // 2022-01-08
      v.setValueCount(1);
    }), LocalDate.ofEpochDay(19_000));
  }

  @Test
  public void testDateMilli() throws IOException {
    Field field = field(new ArrowType.Date(DateUnit.MILLISECOND));
    long midnightMillis = 19_000L * 86_400_000L;
    assertEquals(extract(field, v -> {
      ((DateMilliVector) v).setSafe(0, midnightMillis);
      v.setValueCount(1);
    }), LocalDate.ofEpochDay(19_000));
  }

  @Test
  public void testTimeSecond() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.SECOND, 32));
    assertEquals(extract(field, v -> {
      ((TimeSecVector) v).setSafe(0, 3661); // 01:01:01
      v.setValueCount(1);
    }), LocalTime.of(1, 1, 1));
  }

  @Test
  public void testTimeMilli() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    int midnightOffsetMillis = 3_661_500;
    assertEquals(extract(field, v -> {
      ((TimeMilliVector) v).setSafe(0, midnightOffsetMillis);
      v.setValueCount(1);
    }), LocalTime.of(1, 1, 1, 500_000_000));
  }

  @Test
  public void testTimeMicro() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.MICROSECOND, 64));
    long micros = 3_661_000_500L; // 01:01:01.0005
    assertEquals(extract(field, v -> {
      ((TimeMicroVector) v).setSafe(0, micros);
      v.setValueCount(1);
    }), LocalTime.of(1, 1, 1, 500_000));
  }

  @Test
  public void testTimeNano() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.NANOSECOND, 64));
    long nanos = 3_661_000_000_007L; // 01:01:01.000000007
    assertEquals(extract(field, v -> {
      ((TimeNanoVector) v).setSafe(0, nanos);
      v.setValueCount(1);
    }), LocalTime.of(1, 1, 1, 7));
  }

  @Test
  public void testTimestampSecondNoTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.SECOND, null));
    long sec = 1_700_000_000L;
    assertEquals(extract(field, v -> {
      ((TimeStampSecVector) v).setSafe(0, sec);
      v.setValueCount(1);
    }), new Timestamp(sec * 1000L));
  }

  @Test
  public void testTimestampMilliNoTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
    long ms = 1_700_000_000_500L;
    assertEquals(extract(field, v -> {
      ((TimeStampMilliVector) v).setSafe(0, ms);
      v.setValueCount(1);
    }), new Timestamp(ms));
  }

  @Test
  public void testTimestampMicroNoTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));
    long micros = 1_700_000_000_000_007L;
    Timestamp expected = new Timestamp(1_700_000_000_000L);
    expected.setNanos(7_000);
    assertEquals(extract(field, v -> {
      ((TimeStampMicroVector) v).setSafe(0, micros);
      v.setValueCount(1);
    }), expected);
  }

  @Test
  public void testTimestampNanoNoTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
    long nanos = 1_700_000_000_000_000_007L;
    Timestamp expected = new Timestamp(1_700_000_000_000L);
    expected.setNanos(7);
    assertEquals(extract(field, v -> {
      ((TimeStampNanoVector) v).setSafe(0, nanos);
      v.setValueCount(1);
    }), expected);
  }

  @Test
  public void testTimestampSecondWithTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"));
    long sec = 1_700_000_000L;
    assertEquals(extract(field, v -> {
      ((TimeStampSecTZVector) v).setSafe(0, sec);
      v.setValueCount(1);
    }), new Timestamp(sec * 1000L));
  }

  @Test
  public void testTimestampMilliWithTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
    long ms = 1_700_000_000_500L;
    assertEquals(extract(field, v -> {
      ((TimeStampMilliTZVector) v).setSafe(0, ms);
      v.setValueCount(1);
    }), new Timestamp(ms));
  }

  @Test
  public void testTimestampMicroWithTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
    long micros = 1_700_000_000_000_007L;
    Timestamp expected = new Timestamp(1_700_000_000_000L);
    expected.setNanos(7_000);
    assertEquals(extract(field, v -> {
      ((TimeStampMicroTZVector) v).setSafe(0, micros);
      v.setValueCount(1);
    }), expected);
  }

  @Test
  public void testTimestampNanoWithTZ() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"));
    long nanos = 1_700_000_000_000_000_007L;
    Timestamp expected = new Timestamp(1_700_000_000_000L);
    expected.setNanos(7);
    assertEquals(extract(field, v -> {
      ((TimeStampNanoTZVector) v).setSafe(0, nanos);
      v.setValueCount(1);
    }), expected);
  }

  // === Temporal (raw mode — extractRawTimeValues = true) ===

  @Test
  public void testDateDayRaw() throws IOException {
    Field field = field(new ArrowType.Date(DateUnit.DAY));
    assertEquals(extractRaw(field, v -> {
      ((DateDayVector) v).setSafe(0, 19_000);
      v.setValueCount(1);
    }), 19_000);
  }

  @Test
  public void testDateMilliRaw() throws IOException {
    // Raw mode normalizes to int days regardless of underlying unit.
    Field field = field(new ArrowType.Date(DateUnit.MILLISECOND));
    long midnightMillis = 19_000L * 86_400_000L;
    assertEquals(extractRaw(field, v -> {
      ((DateMilliVector) v).setSafe(0, midnightMillis);
      v.setValueCount(1);
    }), 19_000);
  }

  @Test
  public void testTimeSecondRaw() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.SECOND, 32));
    assertEquals(extractRaw(field, v -> {
      ((TimeSecVector) v).setSafe(0, 3661);
      v.setValueCount(1);
    }), 3661);
  }

  @Test
  public void testTimeMilliRaw() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    int millis = 3_661_500;
    assertEquals(extractRaw(field, v -> {
      ((TimeMilliVector) v).setSafe(0, millis);
      v.setValueCount(1);
    }), millis);
  }

  @Test
  public void testTimeMicroRaw() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.MICROSECOND, 64));
    long micros = 3_661_000_500L;
    assertEquals(extractRaw(field, v -> {
      ((TimeMicroVector) v).setSafe(0, micros);
      v.setValueCount(1);
    }), micros);
  }

  @Test
  public void testTimeNanoRaw() throws IOException {
    Field field = field(new ArrowType.Time(TimeUnit.NANOSECOND, 64));
    long nanos = 3_661_000_000_007L;
    assertEquals(extractRaw(field, v -> {
      ((TimeNanoVector) v).setSafe(0, nanos);
      v.setValueCount(1);
    }), nanos);
  }

  @Test
  public void testTimestampSecondNoTZRaw() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.SECOND, null));
    long sec = 1_700_000_000L;
    assertEquals(extractRaw(field, v -> {
      ((TimeStampSecVector) v).setSafe(0, sec);
      v.setValueCount(1);
    }), sec);
  }

  @Test
  public void testTimestampMilliNoTZRaw() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
    long ms = 1_700_000_000_500L;
    assertEquals(extractRaw(field, v -> {
      ((TimeStampMilliVector) v).setSafe(0, ms);
      v.setValueCount(1);
    }), ms);
  }

  @Test
  public void testTimestampMicroNoTZRaw() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));
    long micros = 1_700_000_000_000_007L;
    assertEquals(extractRaw(field, v -> {
      ((TimeStampMicroVector) v).setSafe(0, micros);
      v.setValueCount(1);
    }), micros);
  }

  @Test
  public void testTimestampNanoNoTZRaw() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
    long nanos = 1_700_000_000_000_000_007L;
    assertEquals(extractRaw(field, v -> {
      ((TimeStampNanoVector) v).setSafe(0, nanos);
      v.setValueCount(1);
    }), nanos);
  }

  @Test
  public void testTimestampMilliWithTZRaw() throws IOException {
    Field field = field(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
    long ms = 1_700_000_000_500L;
    assertEquals(extractRaw(field, v -> {
      ((TimeStampMilliTZVector) v).setSafe(0, ms);
      v.setValueCount(1);
    }), ms);
  }

  // === Interval / Duration ===

  @Test
  public void testIntervalDay() throws IOException {
    Field field = field(new ArrowType.Interval(IntervalUnit.DAY_TIME));
    Object result = extract(field, v -> {
      ((IntervalDayVector) v).setSafe(0, 1, 5_000); // 1 day + 5s
      v.setValueCount(1);
    });
    assertEquals(result, Duration.ofDays(1).plusSeconds(5).toString());
  }

  @Test
  public void testIntervalYear() throws IOException {
    Field field = field(new ArrowType.Interval(IntervalUnit.YEAR_MONTH));
    Object result = extract(field, v -> {
      ((IntervalYearVector) v).setSafe(0, 14); // 14 months — Arrow returns Period.ofMonths (un-normalized)
      v.setValueCount(1);
    });
    assertEquals(result, Period.ofMonths(14).toString());
  }

  @Test
  public void testDuration() throws IOException {
    Field field = field(new ArrowType.Duration(TimeUnit.MILLISECOND));
    Object result = extract(field, v -> {
      ((DurationVector) v).setSafe(0, 90_000L); // 1m30s
      v.setValueCount(1);
    });
    assertEquals(result, Duration.ofSeconds(90).toString());
  }

  // === Complex ===

  @Test
  public void testListOfInt() throws IOException {
    Field elementField = new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field listField = new Field(COLUMN, FieldType.nullable(new ArrowType.List()), List.of(elementField));
    Object result = extract(listField, v -> {
      ListVector lv = (ListVector) v;
      lv.allocateNew();
      IntVector child = (IntVector) lv.getDataVector();
      lv.startNewValue(0);
      child.setSafe(0, 10);
      child.setSafe(1, 20);
      child.setSafe(2, 30);
      lv.endValue(0, 3);
      child.setValueCount(3);
      lv.setValueCount(1);
    });
    assertEquals((Object[]) result, new Object[]{10, 20, 30});
  }

  @Test
  public void testListOfString() throws IOException {
    Field elementField = new Field("$data$", FieldType.nullable(new ArrowType.Utf8()), null);
    Field listField = new Field(COLUMN, FieldType.nullable(new ArrowType.List()), List.of(elementField));
    Object result = extract(listField, v -> {
      ListVector lv = (ListVector) v;
      lv.allocateNew();
      VarCharVector child = (VarCharVector) lv.getDataVector();
      lv.startNewValue(0);
      child.setSafe(0, "a".getBytes());
      child.setSafe(1, "b".getBytes());
      lv.endValue(0, 2);
      child.setValueCount(2);
      lv.setValueCount(1);
    });
    assertEquals((Object[]) result, new Object[]{"a", "b"});
  }

  @Test
  public void testStruct() throws IOException {
    Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field ageField = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field structField = new Field(COLUMN, FieldType.nullable(new ArrowType.Struct()), List.of(nameField, ageField));
    Object result = extract(structField, v -> {
      StructVector sv = (StructVector) v;
      sv.allocateNew();
      ((VarCharVector) sv.getChild("name")).setSafe(0, "Alice".getBytes());
      ((IntVector) sv.getChild("age")).setSafe(0, 30);
      sv.setIndexDefined(0);
      sv.setValueCount(1);
    });
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) result;
    assertEquals(map.get("name"), "Alice");
    assertEquals(map.get("age"), 30);
  }

  @Test
  public void testMap() throws IOException {
    Field keyField = new Field(MapVector.KEY_NAME, FieldType.notNullable(new ArrowType.Utf8()), null);
    Field valField = new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field entriesField =
        new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(new ArrowType.Struct()),
            List.of(keyField, valField));
    Field mapField =
        new Field(COLUMN, FieldType.nullable(new ArrowType.Map(false)), List.of(entriesField));
    Object result = extract(mapField, v -> {
      MapVector mv = (MapVector) v;
      mv.allocateNew();
      StructVector entries = (StructVector) mv.getDataVector();
      VarCharVector keys = (VarCharVector) entries.getChild(MapVector.KEY_NAME);
      IntVector vals = (IntVector) entries.getChild(MapVector.VALUE_NAME);
      mv.startNewValue(0);
      keys.setSafe(0, "k1".getBytes());
      vals.setSafe(0, 100);
      entries.setIndexDefined(0);
      keys.setSafe(1, "k2".getBytes());
      vals.setSafe(1, 200);
      entries.setIndexDefined(1);
      mv.endValue(0, 2);
      keys.setValueCount(2);
      vals.setValueCount(2);
      entries.setValueCount(2);
      mv.setValueCount(1);
    });
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) result;
    assertEquals(map.get("k1"), 100);
    assertEquals(map.get("k2"), 200);
  }

  // === Null value (column with nullable type whose row 0 is null) ===

  @Test
  public void testNullIntValue() throws IOException {
    Field field = field(new ArrowType.Int(32, true));
    assertNull(extract(field, v -> {
      v.setValueCount(1);
    }));
  }

  @Test
  public void testNullStringValue() throws IOException {
    Field field = field(new ArrowType.Utf8());
    assertNull(extract(field, v -> {
      v.setValueCount(1);
    }));
  }

  // === Dictionary-encoded ===

  @Test
  public void testDictionaryEncodedString() throws IOException {
    DictionaryEncoding encoding = new DictionaryEncoding(1L, false, new ArrowType.Int(32, true));

    // Dictionary values: ["Alpha", "Beta", "Gamma"].
    VarCharVector dictVec = new VarCharVector("dict", _allocator);
    dictVec.allocateNew();
    dictVec.setSafe(0, "Alpha".getBytes());
    dictVec.setSafe(1, "Beta".getBytes());
    dictVec.setSafe(2, "Gamma".getBytes());
    dictVec.setValueCount(3);
    Dictionary dictionary = new Dictionary(dictVec, encoding);

    // Encode "Beta" (index 1) for row 0.
    VarCharVector unencoded = new VarCharVector(COLUMN, _allocator);
    unencoded.allocateNew();
    unencoded.setSafe(0, "Beta".getBytes());
    unencoded.setValueCount(1);

    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary);

    try (FieldVector encodedVec = (FieldVector) DictionaryEncoder.encode(unencoded, dictionary)) {
      unencoded.close();
      try (VectorSchemaRoot root =
          new VectorSchemaRoot(List.of(encodedVec.getField()), List.of(encodedVec), 1)) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (WritableByteChannel ch = Channels.newChannel(baos);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, ch)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
        try (ArrowStreamReader reader =
            new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), _allocator)) {
          reader.loadNextBatch();
          ArrowRecordExtractor extractor = new ArrowRecordExtractor();
          extractor.init(null, null);
          extractor.setReader(reader);
          try (ArrowRecordExtractor.Record record = new ArrowRecordExtractor.Record()) {
            extractor.prepareBatch(record);
            record.setRowId(0);
            GenericRow row = new GenericRow();
            extractor.extract(record, row);
            assertEquals(row.getValue(COLUMN), "Beta");
          }
        }
      }
    } finally {
      dictVec.close();
    }
  }

  // === Include-list filter ===

  @Test
  public void testIncludeListFiltersFields() throws IOException {
    Field a = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field b = new Field("b", FieldType.nullable(new ArrowType.Utf8()), null);
    Schema schema = new Schema(List.of(a, b));
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, _allocator)) {
      ((IntVector) root.getVector("a")).setSafe(0, 7);
      ((VarCharVector) root.getVector("b")).setSafe(0, "skipped".getBytes());
      root.getVector("a").setValueCount(1);
      root.getVector("b").setValueCount(1);
      root.setRowCount(1);

      GenericRow row = roundtripAndExtract(root, Set.of("a"), null);
      assertEquals(row.getValue("a"), 7);
      assertNull(row.getValue("b"));
    }
  }

  // === Helpers ===

  private static Field field(ArrowType type) {
    return new Field(COLUMN, FieldType.nullable(type), null);
  }

  private Object extract(Field field, Consumer<FieldVector> populator) throws IOException {
    return extract(field, populator, null);
  }

  private Object extractRaw(Field field, Consumer<FieldVector> populator) throws IOException {
    ArrowRecordExtractorConfig config = new ArrowRecordExtractorConfig();
    config.setExtractRawTimeValues(true);
    return extract(field, populator, config);
  }

  /// Build a single-column root with one populated row, roundtrip through Arrow IPC, then extract row 0.
  private Object extract(Field field, Consumer<FieldVector> populator,
      @Nullable ArrowRecordExtractorConfig config) throws IOException {
    Schema schema = new Schema(List.of(field));
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, _allocator)) {
      FieldVector vector = root.getVector(COLUMN);
      populator.accept(vector);
      root.setRowCount(vector.getValueCount());
      return roundtripAndExtract(root, null, config).getValue(COLUMN);
    }
  }

  private GenericRow roundtripAndExtract(VectorSchemaRoot root, @Nullable Set<String> fieldsToRead,
      @Nullable ArrowRecordExtractorConfig config) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (WritableByteChannel ch = Channels.newChannel(baos);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, ch)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    try (ArrowStreamReader reader = new ArrowStreamReader(
        new ByteArrayInputStream(baos.toByteArray()), _allocator)) {
      reader.loadNextBatch();
      ArrowRecordExtractor extractor = new ArrowRecordExtractor();
      extractor.init(fieldsToRead, config);
      extractor.setReader(reader);
      try (ArrowRecordExtractor.Record record = new ArrowRecordExtractor.Record()) {
        extractor.prepareBatch(record);
        record.setRowId(0);
        GenericRow row = new GenericRow();
        extractor.extract(record, row);
        return row;
      }
    }
  }
}
