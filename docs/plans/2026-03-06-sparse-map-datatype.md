# Native SPARSE_MAP Datatype Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a native SPARSE_MAP datatype to Apache Pinot that stores sparse key-value data using per-key columnar storage, enabling O(1) typed key lookup, direct group-by, and bitmap-based filtering without full deserialization.

**Architecture:** SPARSE_MAP is a new DataType declared as a DIMENSION field with per-key type metadata in `DimensionFieldSpec.sparseMapKeyTypes`. The SparseMapIndex is the sole storage (no forward index blob). Each key gets its own presence bitmap (RoaringBitmap), typed forward index, and optional inverted index. Query access uses the existing bracket syntax `col['key']` via `ItemTransformFunction` → `SparseMapDataSource.getKeyDataSource()`.

**Tech Stack:** Java 11, RoaringBitmap, PinotDataBuffer (memory-mapped I/O), @AutoService (SPI plugin registration), Jackson (JSON schema serialization)

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| FieldType | DIMENSION | Clean DataSource integration, typed group-by support |
| Stored type | SPARSE_MAP (self-referential) | Prevents MAP code paths from accidentally handling SPARSE_MAP |
| Storage | SparseMapIndex only | No forward index blob — saves storage for sparse data |
| Undeclared keys | Stored as STRING (configurable via sparseMapDefaultValueType) | Flexibility for schema evolution |
| Query syntax | Reuse bracket notation `col['key']` | Minimal user-facing change |
| Null semantics | NULL for absent keys | Presence bitmap = inverted null bitmap |
| Multi-value | Single-value only per key | Simpler storage, covers sparse column use case |

---

## Task 1: Add SPARSE_MAP to DataType Enum

**Files:**
- Modify: `pinot-spi/src/main/java/org/apache/pinot/spi/data/FieldSpec.java`

**Step 1: Add enum constant**

After line 632 (`MAP(false, false),`), add:

```java
SPARSE_MAP(false, false),
```

This gives `SPARSE_MAP.getStoredType() == SPARSE_MAP` (self-referential, using the 2-arg constructor).

**Step 2: Update convert() method**

In `convert()` (line 708), add before the `default:` case:

```java
case SPARSE_MAP:
  return JsonUtils.stringToObject(value, Map.class);
```

**Step 3: Update compare() method**

In `compare()` (line 757), add alongside MAP/LIST:

```java
case MAP:
case LIST:
case SPARSE_MAP:
  throw new UnsupportedOperationException("Cannot compare complex data types: " + this);
```

**Step 4: Update toString(Object) method**

In `toString(Object)` (line 796), update the MAP/LIST check:

```java
if (this == MAP || this == LIST || this == SPARSE_MAP) {
```

**Step 5: Update convertInternal() method**

In `convertInternal()` (line 831), add alongside MAP/LIST:

```java
case MAP:
case LIST:
case SPARSE_MAP:
  throw new UnsupportedOperationException("Cannot convert complex data types: " + this);
```

**Step 6: Add default null value constant and update getDefaultNullValue()**

After line 97 (`DEFAULT_COMPLEX_NULL_VALUE_OF_MAP`), add:

```java
public static final Map DEFAULT_DIMENSION_NULL_VALUE_OF_SPARSE_MAP = Map.of();
```

In `getDefaultNullValue()` (line 412), in the DIMENSION/TIME/DATE_TIME case, add:

```java
case SPARSE_MAP:
  return DEFAULT_DIMENSION_NULL_VALUE_OF_SPARSE_MAP;
```

**Step 7: Verify and run**

Run: `mvn compile -pl pinot-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 8: Commit**

```bash
git add pinot-spi/src/main/java/org/apache/pinot/spi/data/FieldSpec.java
git commit -m "feat: add SPARSE_MAP to DataType enum with self-referential stored type"
```

---

## Task 2: Extend DimensionFieldSpec with Key Type Metadata

**Files:**
- Modify: `pinot-spi/src/main/java/org/apache/pinot/spi/data/DimensionFieldSpec.java`

**Step 1: Add sparseMapKeyTypes and sparseMapDefaultValueType fields**

Add imports and fields:

```java
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class DimensionFieldSpec extends FieldSpec {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Map<String, DataType> _sparseMapKeyTypes;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private DataType _sparseMapDefaultValueType;
```

**Step 2: Add getters and setters**

```java
public Map<String, DataType> getSparseMapKeyTypes() {
  return _sparseMapKeyTypes;
}

public void setSparseMapKeyTypes(Map<String, DataType> sparseMapKeyTypes) {
  _sparseMapKeyTypes = sparseMapKeyTypes;
}

public DataType getSparseMapDefaultValueType() {
  return _sparseMapDefaultValueType;
}

public void setSparseMapDefaultValueType(DataType sparseMapDefaultValueType) {
  _sparseMapDefaultValueType = sparseMapDefaultValueType;
}
```

**Step 3: Verify and run**

Run: `mvn compile -pl pinot-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add pinot-spi/src/main/java/org/apache/pinot/spi/data/DimensionFieldSpec.java
git commit -m "feat: add sparseMapKeyTypes and sparseMapDefaultValueType to DimensionFieldSpec"
```

---

## Task 3: Update Schema Validation

**Files:**
- Modify: `pinot-spi/src/main/java/org/apache/pinot/spi/data/Schema.java`

**Step 1: Allow SPARSE_MAP in DIMENSION validation**

In `Schema.validate()` (line 115), in the DIMENSION/TIME/DATE_TIME switch, add `case SPARSE_MAP:` after `case BYTES:` (line 130):

```java
case BYTES:
case SPARSE_MAP:
  break;
```

**Step 2: Add _hasSparseMapColumn flag**

After line 92 (`private boolean _hasJSONColumn;`), add:

```java
private boolean _hasSparseMapColumn;
```

Add getter:

```java
public boolean hasSparseMapColumn() {
  return _hasSparseMapColumn;
}
```

**Step 3: Set flag in addField()**

Find the `addField()` method. After the line that sets `_hasJSONColumn`, add:

```java
if (fieldSpec.getDataType() == DataType.SPARSE_MAP) {
  _hasSparseMapColumn = true;
}
```

**Step 4: Add SPARSE_MAP-specific validation**

In the `validate()` method or in a new `validateSparseMapField()` helper, add:

```java
if (fieldSpec.getDataType() == DataType.SPARSE_MAP) {
  Preconditions.checkState(fieldSpec.isSingleValueField(),
      "SPARSE_MAP column '%s' must be single-value", fieldSpec.getName());
  Preconditions.checkState(fieldSpec instanceof DimensionFieldSpec,
      "SPARSE_MAP column '%s' must be a DIMENSION field", fieldSpec.getName());
  DimensionFieldSpec dimSpec = (DimensionFieldSpec) fieldSpec;
  Preconditions.checkState(dimSpec.getSparseMapKeyTypes() != null && !dimSpec.getSparseMapKeyTypes().isEmpty(),
      "SPARSE_MAP column '%s' must declare sparseMapKeyTypes", fieldSpec.getName());
  for (Map.Entry<String, DataType> entry : dimSpec.getSparseMapKeyTypes().entrySet()) {
    DataType valueType = entry.getValue();
    Preconditions.checkState(
        valueType == DataType.INT || valueType == DataType.LONG || valueType == DataType.FLOAT
            || valueType == DataType.DOUBLE || valueType == DataType.STRING || valueType == DataType.BYTES,
        "SPARSE_MAP column '%s' key '%s' has unsupported value type: %s",
        fieldSpec.getName(), entry.getKey(), valueType);
  }
}
```

**Step 5: Verify and run**

Run: `mvn compile -pl pinot-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 6: Commit**

```bash
git add pinot-spi/src/main/java/org/apache/pinot/spi/data/Schema.java
git commit -m "feat: allow SPARSE_MAP in DIMENSION schema validation with key type checks"
```

---

## Task 4: Update NullValuePlaceHolder

**Files:**
- Modify: `pinot-spi/src/main/java/org/apache/pinot/spi/utils/CommonConstants.java`

**Step 1: Add SPARSE_MAP placeholder**

After line 2120 (`public static final Object MAP = Collections.emptyMap();`), add:

```java
public static final Object SPARSE_MAP = Collections.emptyMap();
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-spi/src/main/java/org/apache/pinot/spi/utils/CommonConstants.java
git commit -m "feat: add SPARSE_MAP NullValuePlaceHolder"
```

---

## Task 5: Add SPARSE_MAP to ColumnDataType (DataSchema)

**Files:**
- Modify: `pinot-common/src/main/java/org/apache/pinot/common/utils/DataSchema.java`

**Step 1: Add SPARSE_MAP enum constant**

After the `MAP` entry (line 298), add:

```java
SPARSE_MAP(NullValuePlaceHolder.SPARSE_MAP) {
  @Override
  public RelDataType toType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.MAP);
  }
},
```

**Step 2: Update toDataType()**

In `toDataType()` (line 428), add before the default case:

```java
case MAP:
  return DataType.MAP;
case SPARSE_MAP:
  return DataType.SPARSE_MAP;
```

**Step 3: Update fromDataTypeSV()**

In `fromDataTypeSV()` (line 827), add after `case MAP: return MAP;`:

```java
case SPARSE_MAP:
  return SPARSE_MAP;
```

**Step 4: Update convert()**

In `convert()` (line 556), add before the default case:

```java
case MAP:
case SPARSE_MAP:
  return (Serializable) value;
```

**Step 5: Update convertAndFormat()**

In `convertAndFormat()` (line 626), update the MAP case:

```java
case MAP:
case SPARSE_MAP:
  return toMap(value);
```

**Step 6: Verify and run**

Run: `mvn compile -pl pinot-common -am -DskipTests`
Expected: BUILD SUCCESS

**Step 7: Commit**

```bash
git add pinot-common/src/main/java/org/apache/pinot/common/utils/DataSchema.java
git commit -m "feat: add SPARSE_MAP to ColumnDataType with conversion methods"
```

---

## Task 6: Add SPARSE_MAP to PinotDataType

**Files:**
- Modify: `pinot-common/src/main/java/org/apache/pinot/common/utils/PinotDataType.java`

**Step 1: Add SPARSE_MAP enum value**

Model after the existing MAP enum. Add after the MAP entry:

```java
SPARSE_MAP {
  @Override
  public int toInt(Object value) {
    return unsupported();
  }

  @Override
  public long toLong(Object value) {
    return unsupported();
  }

  @Override
  public float toFloat(Object value) {
    return unsupported();
  }

  @Override
  public double toDouble(Object value) {
    return unsupported();
  }

  @Override
  public boolean toBoolean(Object value) {
    return unsupported();
  }

  @Override
  public Timestamp toTimestamp(Object value) {
    return unsupported();
  }

  @Override
  public String toString(Object value) {
    try {
      return JsonUtils.objectToString(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] toBytes(Object value) {
    return MapUtils.serializeMap((Map) value);
  }

  @Override
  public BigDecimal toBigDecimal(Object value) {
    return unsupported();
  }

  @Override
  public Object convert(Object value, PinotDataType sourceType) {
    switch (sourceType) {
      case STRING:
        try {
          return JsonUtils.stringToObject((String) value, Map.class);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      case BYTES:
        return MapUtils.deserializeMap((byte[]) value);
      case OBJECT:
      case MAP:
      case SPARSE_MAP:
        return value;
      default:
        return unsupported();
    }
  }
}
```

**Step 2: Update getSingleValueType()**

In `getSingleValueType()`, add:

```java
if (cls == Map.class) {
  // Check if existing code returns MAP, add SPARSE_MAP awareness if needed
  return MAP; // SPARSE_MAP is handled at schema level, not PinotDataType inference
}
```

No change needed here -- the runtime type for SPARSE_MAP values is `Map.class`, same as MAP. The PinotDataType used during ingestion is driven by the schema's DataType, not inferred from the Java class.

**Step 3: Update getPinotDataTypeForIngestion()**

Add:

```java
case SPARSE_MAP:
  return SPARSE_MAP;
```

**Step 4: Verify and run**

Run: `mvn compile -pl pinot-common -am -DskipTests`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add pinot-common/src/main/java/org/apache/pinot/common/utils/PinotDataType.java
git commit -m "feat: add SPARSE_MAP to PinotDataType with conversion support"
```

---

## Task 7: Add V1Constants and StandardIndexes Entries

**Files:**
- Modify: `pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/V1Constants.java`
- Modify: `pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/StandardIndexes.java`

**Step 1: Add file extension constant**

In `V1Constants.Indexes` (after line 48, JSON_INDEX_FILE_EXTENSION), add:

```java
public static final String SPARSE_MAP_INDEX_FILE_EXTENSION = ".sparsemap.idx";
```

**Step 2: Add StandardIndexes entry**

In `StandardIndexes.java`, add the ID constant and accessor:

```java
public static final String SPARSE_MAP_ID = "sparse_map_index";
```

And the accessor method (after the `vector()` method):

```java
public static IndexType<SparseMapIndexConfig, SparseMapIndexReader, SparseMapIndexCreator> sparseMap() {
  return (IndexType<SparseMapIndexConfig, SparseMapIndexReader, SparseMapIndexCreator>)
      IndexService.getInstance().get(SPARSE_MAP_ID);
}
```

Note: This will not compile yet until SparseMapIndexConfig, SparseMapIndexReader, and SparseMapIndexCreator are created in Tasks 8-10. Add the import statements but comment out the method body until those classes exist. Alternatively, use raw types temporarily:

```java
@SuppressWarnings("unchecked")
public static IndexType sparseMap() {
  return IndexService.getInstance().get(SPARSE_MAP_ID);
}
```

**Step 3: Verify and run**

Run: `mvn compile -pl pinot-segment-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/V1Constants.java
git add pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/StandardIndexes.java
git commit -m "feat: add SPARSE_MAP_INDEX constants to V1Constants and StandardIndexes"
```

---

## Task 8: Create SparseMapIndexConfig

**Files:**
- Create: `pinot-spi/src/main/java/org/apache/pinot/spi/config/table/SparseMapIndexConfig.java`

**Step 1: Create config class**

```java
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Configuration for the SparseMap index on a SPARSE_MAP column.
 * Controls which keys are indexed and whether per-key inverted indexes are enabled.
 */
public class SparseMapIndexConfig extends IndexConfig {
  public static final SparseMapIndexConfig DISABLED = new SparseMapIndexConfig(false);
  public static final SparseMapIndexConfig DEFAULT = new SparseMapIndexConfig(true);

  private final Set<String> _indexedKeys;
  private final boolean _enableInvertedIndex;
  private final int _maxKeys;

  public SparseMapIndexConfig(boolean enabled) {
    this(enabled, null, false, 1000);
  }

  @JsonCreator
  public SparseMapIndexConfig(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("indexedKeys") @Nullable Set<String> indexedKeys,
      @JsonProperty("enableInvertedIndex") boolean enableInvertedIndex,
      @JsonProperty("maxKeys") int maxKeys) {
    super(enabled);
    _indexedKeys = indexedKeys;
    _enableInvertedIndex = enableInvertedIndex;
    _maxKeys = maxKeys > 0 ? maxKeys : 1000;
  }

  @Nullable
  public Set<String> getIndexedKeys() {
    return _indexedKeys;
  }

  public boolean isEnableInvertedIndex() {
    return _enableInvertedIndex;
  }

  public int getMaxKeys() {
    return _maxKeys;
  }
}
```

**Step 2: Add to IndexingConfig**

Modify: `pinot-spi/src/main/java/org/apache/pinot/spi/config/table/IndexingConfig.java`

Add field:

```java
private Map<String, SparseMapIndexConfig> _sparseMapIndexConfigs;
```

Add getter/setter:

```java
public Map<String, SparseMapIndexConfig> getSparseMapIndexConfigs() {
  return _sparseMapIndexConfigs;
}

public void setSparseMapIndexConfigs(Map<String, SparseMapIndexConfig> sparseMapIndexConfigs) {
  _sparseMapIndexConfigs = sparseMapIndexConfigs;
}
```

**Step 3: Verify and run**

Run: `mvn compile -pl pinot-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add pinot-spi/src/main/java/org/apache/pinot/spi/config/table/SparseMapIndexConfig.java
git add pinot-spi/src/main/java/org/apache/pinot/spi/config/table/IndexingConfig.java
git commit -m "feat: create SparseMapIndexConfig and add to IndexingConfig"
```

---

## Task 9: Create SparseMapIndexCreator Interface

**Files:**
- Create: `pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/creator/SparseMapIndexCreator.java`

**Step 1: Create interface**

```java
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import java.util.Map;
import org.apache.pinot.segment.spi.index.IndexCreator;

/**
 * Creator for the SparseMap index. Accepts a Map per document during segment creation,
 * decomposes it into per-key columnar storage on seal().
 */
public interface SparseMapIndexCreator extends IndexCreator {

  /**
   * Adds a document's sparse map data. The map may be null or empty if the document has no keys.
   */
  void add(Map<String, Object> sparseMap) throws IOException;

  /**
   * Finalizes the index, writing per-key presence bitmaps, forward indexes, and optional inverted indexes.
   */
  void seal() throws IOException;
}
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/creator/SparseMapIndexCreator.java
git commit -m "feat: create SparseMapIndexCreator interface"
```

---

## Task 10: Create SparseMapIndexReader Interface

**Files:**
- Create: `pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/reader/SparseMapIndexReader.java`

**Step 1: Create interface**

```java
package org.apache.pinot.segment.spi.index.reader;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * Reader for the SparseMap index. Provides O(1) typed key lookup per document
 * via presence bitmap rank operations.
 */
public interface SparseMapIndexReader extends IndexReader {

  Set<String> getKeys();

  DataType getKeyValueType(String key);

  int getNumDocsWithKey(String key);

  ImmutableRoaringBitmap getPresenceBitmap(String key);

  // Typed value access — O(1) per doc via presence bitmap rank
  int getInt(int docId, String key);
  long getLong(int docId, String key);
  float getFloat(int docId, String key);
  double getDouble(int docId, String key);
  String getString(int docId, String key);
  byte[] getBytes(int docId, String key);

  /**
   * Returns a bitmap of docIds that have the given key-value pair.
   * Only available if inverted index is enabled for the key.
   * Returns null if inverted index is not available.
   */
  @Nullable
  ImmutableRoaringBitmap getDocsWithKeyValue(String key, Object value);

  /**
   * Returns a DataSource for the given key, suitable for use with
   * ItemTransformFunction and MapFilterOperator.
   */
  DataSource getKeyDataSource(String key);

  /**
   * Reconstructs the full map for a document from per-key data.
   * Used for SELECT sparse_col queries and data table transport.
   */
  Map<String, Object> getMap(int docId);
}
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-spi -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-spi/src/main/java/org/apache/pinot/segment/spi/index/reader/SparseMapIndexReader.java
git commit -m "feat: create SparseMapIndexReader interface with O(1) typed key access"
```

---

## Task 11: Create SparseMapIndexType and SparseMapIndexPlugin

**Files:**
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexType.java`
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexPlugin.java`

**Step 1: Create SparseMapIndexType**

Model after `JsonIndexType.java`. Key differences: validates SPARSE_MAP dataType (not storedType STRING/MAP), and the index is mandatory for SPARSE_MAP columns.

```java
package org.apache.pinot.segment.local.segment.index.sparsemap;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.SparseMapIndexHandler;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.SparseMapIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.SparseMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.SparseMapIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;

public class SparseMapIndexType
    extends AbstractIndexType<SparseMapIndexConfig, SparseMapIndexReader, SparseMapIndexCreator> {

  public static final String INDEX_DISPLAY_NAME = "sparse_map";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.SPARSE_MAP_INDEX_FILE_EXTENSION);

  protected SparseMapIndexType() {
    super(StandardIndexes.SPARSE_MAP_ID);
  }

  @Override
  public Class<SparseMapIndexConfig> getIndexConfigClass() {
    return SparseMapIndexConfig.class;
  }

  @Override
  public SparseMapIndexConfig getDefaultConfig() {
    return SparseMapIndexConfig.DISABLED;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    SparseMapIndexConfig config = indexConfigs.getConfig(StandardIndexes.sparseMap());
    if (config.isEnabled()) {
      String column = fieldSpec.getName();
      Preconditions.checkState(fieldSpec.isSingleValueField(),
          "Cannot create SparseMap index on multi-value column: %s", column);
      Preconditions.checkState(fieldSpec.getDataType() == DataType.SPARSE_MAP,
          "SparseMap index can only be created on SPARSE_MAP columns, got: %s for column: %s",
          fieldSpec.getDataType(), column);
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<SparseMapIndexConfig> createDeserializerForLegacyConfigs() {
    return IndexConfigDeserializer.fromMap(
        tableConfig -> tableConfig.getIndexingConfig().getSparseMapIndexConfigs());
  }

  @Override
  public SparseMapIndexCreator createIndexCreator(IndexCreationContext context, SparseMapIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().getDataType() == DataType.SPARSE_MAP,
        "SparseMap index requires SPARSE_MAP data type");
    return new OnHeapSparseMapIndexCreator(context, indexConfig);
  }

  @Override
  protected IndexReaderFactory<SparseMapIndexReader> createReaderFactory() {
    return new IndexReaderFactory.Default<>() {
      @Override
      protected SparseMapIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
        return new ImmutableSparseMapIndexReader(dataBuffer, metadata);
      }
    };
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, SparseMapIndexConfig config) {
    if (config.isEnabled() && context.getFieldSpec().getDataType() == DataType.SPARSE_MAP) {
      return new MutableSparseMapIndexImpl(context, config);
    }
    return null;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory,
      Map<String, FieldIndexConfigs> configsByCol, @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new SparseMapIndexHandler(segmentDirectory, configsByCol, schema, tableConfig);
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }
}
```

**Step 2: Create SparseMapIndexPlugin**

```java
package org.apache.pinot.segment.local.segment.index.sparsemap;

import com.google.auto.service.AutoService;
import org.apache.pinot.segment.spi.index.IndexPlugin;

@AutoService(IndexPlugin.class)
public class SparseMapIndexPlugin implements IndexPlugin<SparseMapIndexType> {
  public static final SparseMapIndexType INSTANCE = new SparseMapIndexType();

  @Override
  public SparseMapIndexType getIndexType() {
    return INSTANCE;
  }
}
```

**Step 3: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS (may have errors due to missing implementation classes — create stub implementations first if needed)

**Step 4: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/
git commit -m "feat: create SparseMapIndexType and SparseMapIndexPlugin with @AutoService registration"
```

---

## Task 12: Implement OnHeapSparseMapIndexCreator

**Files:**
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/OnHeapSparseMapIndexCreator.java`

This is the core of the immutable index creation. During segment creation, it collects per-key data, then on `seal()` writes the binary index file.

**Step 1: Implement the creator**

Key data structures during creation:
- `Map<String, DataType> keyTypes` — from schema + discovered keys
- `Map<String, MutableRoaringBitmap> presenceBitmaps` — per-key
- `Map<String, List<Object>> values` — per-key typed values (ordinal-indexed)
- `int numDocs` — total documents processed

On `seal()`, write the index file with this layout:

```
[Header - 64 bytes]
  version: int (1)
  numKeys: int
  numDocs: int
  keyDictionaryOffset: long
  keyMetadataOffset: long
  perKeyDataOffset: long
  reserved: padding to 64 bytes

[Key Dictionary Section]
  numKeys: int
  For each key (sorted alphabetically):
    keyLength: int
    keyBytes: byte[keyLength]  (UTF-8)

[Key Metadata Section — one entry per key, 57 bytes each]
  valueDataType: byte (DataType ordinal)
  numDocsWithKey: int
  presenceBitmapOffset: long (relative to perKeyDataSection start)
  presenceBitmapLength: int
  forwardIndexOffset: long
  forwardIndexLength: long
  invertedIndexOffset: long (0 if disabled)
  invertedIndexLength: long

[Per-Key Data Section]
  For each key:
    [Presence Bitmap] — serialized RoaringBitmap
    [Forward Index]
      Fixed-width (INT/LONG/FLOAT/DOUBLE): packed array, indexed by ordinal
      Variable-width (STRING/BYTES): count: int, offsets: int[count+1], data: byte[]
    [Inverted Index] (optional)
      numValues: int
      For each unique value (sorted):
        valueLength: int, valueBytes: byte[], bitmapLength: int, bitmapBytes: byte[]
```

The forward index within each key is indexed by **ordinal** (0..numDocsWithKey-1). Ordinal is computed from the presence bitmap's `rank(docId)`.

Implementation class (~300-400 lines). Key methods:

- `add(Map<String, Object>)` — for each key in the map, set bit in presence bitmap, append typed value to values list
- `seal()` — sort keys, build index file using `DataOutputStream` + `ByteArrayOutputStream`, write to file
- `close()` — cleanup

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/OnHeapSparseMapIndexCreator.java
git commit -m "feat: implement OnHeapSparseMapIndexCreator with per-key columnar storage"
```

---

## Task 13: Implement ImmutableSparseMapIndexReader

**Files:**
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/ImmutableSparseMapIndexReader.java`

Memory-maps the index file via `PinotDataBuffer`. Provides O(1) typed key lookup.

**Step 1: Implement the reader**

Key data structures:
- `Map<String, Integer> keyToId` — O(1) key name to keyId lookup
- `ImmutableRoaringBitmap[] presenceBitmaps` — per-key, loaded from file
- `PinotDataBuffer[] forwardIndexBuffers` — per-key slices of the data buffer
- `DataType[] keyValueTypes` — per-key value types
- `int[] numDocsPerKey` — per-key document counts

Core read path for `getInt(docId, key)`:
```java
public int getInt(int docId, String key) {
  int keyId = keyToId.get(key);
  ImmutableRoaringBitmap bitmap = presenceBitmaps[keyId];
  if (!bitmap.contains(docId)) {
    return 0; // null default
  }
  int ordinal = bitmap.rank(docId) - 1;
  return forwardIndexBuffers[keyId].getInt((long) ordinal * Integer.BYTES);
}
```

`getMap(int docId)` — reconstructs full map by iterating all keys, checking presence, reading values.

`getKeyDataSource(String key)` — returns a `DataSource` backed by this reader's per-key data. This DataSource provides a `ForwardIndexReader` that delegates to this reader's typed accessors. The presence bitmap serves as the inverted null bitmap.

Implementation class (~400-500 lines).

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/ImmutableSparseMapIndexReader.java
git commit -m "feat: implement ImmutableSparseMapIndexReader with O(1) rank-based lookup"
```

---

## Task 14: Implement MutableSparseMapIndexImpl

**Files:**
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/MutableSparseMapIndexImpl.java`

In-memory mutable index for real-time segments. Pattern follows `MutableJsonIndexImpl`.

**Step 1: Implement the mutable index**

Key data structures:
- `Map<String, DataType> keyTypes` — from schema
- `Map<String, MutableRoaringBitmap> presenceBitmaps` — per-key
- `Map<String, IntArrayList/LongArrayList/etc.> typedValues` — per-key, typed primitive arrays
- `Map<String, ObjectArrayList<String>> stringValues` — for STRING keys
- `ReentrantReadWriteLock lock` — for concurrent access
- `long bytesSize` — memory tracking

Implements `MutableIndex` interface. Core method:

```java
public void add(Object value, int dictId, int docId) {
  // value is a Map<String, Object>
  Map<String, Object> map = (Map<String, Object>) value;
  writeLock.lock();
  try {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      if (val == null) continue;
      getOrCreatePresenceBitmap(key).add(docId);
      addTypedValue(key, val);
    }
  } finally {
    writeLock.unlock();
  }
}
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/MutableSparseMapIndexImpl.java
git commit -m "feat: implement MutableSparseMapIndexImpl with concurrent per-key storage"
```

---

## Task 15: Create SparseMapDataSource

**Files:**
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/map/SparseMapDataSource.java`

This DataSource implements `MapDataSource` and delegates to the `SparseMapIndexReader` for per-key access.

**Step 1: Implement SparseMapDataSource**

```java
package org.apache.pinot.segment.local.segment.index.map;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.reader.SparseMapIndexReader;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;

/**
 * DataSource for SPARSE_MAP columns. Implements MapDataSource to provide per-key
 * DataSource access via SparseMapIndexReader.
 */
public class SparseMapDataSource implements MapDataSource {
  private final DimensionFieldSpec _fieldSpec;
  private final SparseMapIndexReader _reader;
  private final DataSourceMetadata _metadata;
  private final Map<String, DataSource> _keyDataSourceCache = new ConcurrentHashMap<>();

  // ... constructor, getKeyDataSource delegates to _reader.getKeyDataSource(key),
  // getKeyDataSources iterates _reader.getKeys()
}
```

Key method:

```java
@Override
public DataSource getKeyDataSource(String key) {
  return _keyDataSourceCache.computeIfAbsent(key, k -> _reader.getKeyDataSource(k));
}
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/map/SparseMapDataSource.java
git commit -m "feat: create SparseMapDataSource implementing MapDataSource for per-key access"
```

---

## Task 16: Implement SparseMapIndexHandler

**Files:**
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/loader/invertedindex/SparseMapIndexHandler.java`

Handles adding/removing SparseMapIndex when table config changes during segment reload.

**Step 1: Implement handler**

Model after `JsonIndexHandler.java`. Key logic:
- If config says SparseMapIndex is enabled but segment doesn't have it: build from forward index data
- If config says SparseMapIndex is disabled but segment has it: remove the index file

Since SPARSE_MAP has no forward index, rebuild must be handled differently: if the index is missing, the segment is invalid (SparseMapIndex is mandatory for SPARSE_MAP). The handler should throw an error if asked to remove the SparseMapIndex from a SPARSE_MAP column.

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/loader/invertedindex/SparseMapIndexHandler.java
git commit -m "feat: implement SparseMapIndexHandler for segment reload"
```

---

## Task 17: Update StatsCollector for SPARSE_MAP

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/creator/impl/stats/StatsCollectorUtil.java`
- Create: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/creator/impl/stats/SparseMapColumnPreIndexStatsCollector.java`

**Step 1: Create SparseMapColumnPreIndexStatsCollector**

Similar to `MapColumnPreIndexStatsCollector` but reads key types from `DimensionFieldSpec.sparseMapKeyTypes` instead of `ComplexFieldSpec.MapFieldSpec`. Tracks per-key stats: cardinality, min/max, null count.

**Step 2: Update StatsCollectorUtil**

In `StatsCollectorUtil` (around line 56, the switch on stored type), add a guard **before** the switch that checks for SPARSE_MAP by dataType:

```java
if (fieldSpec.getDataType() == DataType.SPARSE_MAP) {
  return new SparseMapColumnPreIndexStatsCollector(columnName, statsCollectorConfig,
      (DimensionFieldSpec) fieldSpec);
}
```

This guard must come before the `switch (fieldSpec.getDataType().getStoredType())` to prevent SPARSE_MAP from falling through to the default case (since SPARSE_MAP stored type is SPARSE_MAP, not MAP).

**Step 3: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/creator/impl/stats/
git commit -m "feat: add SparseMapColumnPreIndexStatsCollector and update StatsCollectorUtil"
```

---

## Task 18: Update MutableSegmentImpl for SPARSE_MAP

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/indexsegment/mutable/MutableSegmentImpl.java`

**Step 1: Update isNoDictionaryColumn()**

Around line 559, add SPARSE_MAP alongside MAP:

```java
if (dataType == DataType.MAP || dataType == DataType.SPARSE_MAP) {
  return true;
}
```

**Step 2: Update IndexContainer.toDataSource()**

Around line 1602, add SPARSE_MAP handling:

```java
if (_fieldSpec.getDataType() == DataType.SPARSE_MAP) {
  SparseMapIndexReader sparseMapReader = (SparseMapIndexReader) _mutableIndexes.get(StandardIndexes.sparseMap());
  return new SparseMapDataSource((DimensionFieldSpec) _fieldSpec, sparseMapReader, /* metadata */);
}
```

**Step 3: Ensure forward index is skipped**

SPARSE_MAP columns should not create a forward index. In the index creation flow within MutableSegmentImpl, ensure that when iterating index types, the forward index is skipped for SPARSE_MAP columns. This is controlled by `ForwardIndexConfig.isEnabled()` — ensure the config returns false for SPARSE_MAP.

**Step 4: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/indexsegment/mutable/MutableSegmentImpl.java
git commit -m "feat: update MutableSegmentImpl for SPARSE_MAP no-dict and DataSource creation"
```

---

## Task 19: Update ForwardIndexType to Disable for SPARSE_MAP

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/forward/ForwardIndexType.java`

**Step 1: Auto-disable forward index for SPARSE_MAP**

In `adaptConfig()` or `computeConfig()`, add logic:

```java
if (fieldSpec.getDataType() == DataType.SPARSE_MAP) {
  return ForwardIndexConfig.getDisabled();
}
```

This ensures SPARSE_MAP columns never create a forward index regardless of table config.

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/forward/ForwardIndexType.java
git commit -m "feat: auto-disable forward index for SPARSE_MAP columns"
```

---

## Task 20: Update DataTypeTransformerUtils for Ingestion

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/utils/DataTypeTransformerUtils.java`

**Step 1: Skip standardization for SPARSE_MAP**

Around line 59, add SPARSE_MAP to the skip check:

```java
if (destDataType != PinotDataType.JSON && destDataType != PinotDataType.MAP
    && destDataType != PinotDataType.SPARSE_MAP) {
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/utils/DataTypeTransformerUtils.java
git commit -m "feat: skip standardization for SPARSE_MAP ingestion values"
```

---

## Task 21: Update Query Layer — SelectionOperatorUtils

**Files:**
- Modify: `pinot-core/src/main/java/org/apache/pinot/core/query/selection/SelectionOperatorUtils.java`

**Step 1: Add SPARSE_MAP handling**

Find the MAP handling in this file and add SPARSE_MAP alongside it. SPARSE_MAP values at the query result level are `Map<String, Object>`, same as MAP.

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-core -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-core/src/main/java/org/apache/pinot/core/query/selection/SelectionOperatorUtils.java
git commit -m "feat: add SPARSE_MAP handling to SelectionOperatorUtils"
```

---

## Task 22: Update MapFilterOperator for SparseMapIndex

**Files:**
- Modify: `pinot-core/src/main/java/org/apache/pinot/core/operator/filter/MapFilterOperator.java`

**Step 1: Check for SparseMapIndex before JsonIndex**

In the constructor (around line 62-85), add SparseMapIndex check:

```java
// Check for SparseMapIndex first (O(1) per-key inverted lookup)
SparseMapIndexReader sparseMapReader = dataSource.getSparseMapIndex();
if (sparseMapReader != null) {
  // Use SparseMapIndexReader.getDocsWithKeyValue() for bitmap-based filtering
  _sparseMapReader = sparseMapReader;
  return;
}
// Fall back to JsonIndex
JsonIndexReader jsonReader = ...;
```

When `getDocsWithKeyValue(key, value)` returns a bitmap, wrap it in a `BitmapBasedFilterOperator` for the filter result.

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-core -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-core/src/main/java/org/apache/pinot/core/operator/filter/MapFilterOperator.java
git commit -m "feat: add SparseMapIndex support to MapFilterOperator for bitmap-based filtering"
```

---

## Task 23: Update BaseSegmentCreator for SPARSE_MAP Metadata

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/creator/impl/BaseSegmentCreator.java`

**Step 1: Add SPARSE_MAP metadata handling**

In `addColumnMetadataInfo()`, add SPARSE_MAP handling. Since SPARSE_MAP is a DimensionFieldSpec (not ComplexFieldSpec), the existing COMPLEX handling won't fire. Add:

```java
if (fieldSpec.getDataType() == DataType.SPARSE_MAP) {
  DimensionFieldSpec dimSpec = (DimensionFieldSpec) fieldSpec;
  properties.setProperty(column + ".sparseMapKeyTypes",
      JsonUtils.objectToString(dimSpec.getSparseMapKeyTypes()));
}
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/creator/impl/BaseSegmentCreator.java
git commit -m "feat: add SPARSE_MAP per-key type metadata to segment properties"
```

---

## Task 24: Update TableConfigUtils Validation

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/utils/TableConfigUtils.java`

**Step 1: Add SPARSE_MAP validation rules**

Add validation that prevents:
- Star-tree index on SPARSE_MAP columns
- Standard inverted/range/bloom indexes on SPARSE_MAP columns (use SparseMapIndex instead)
- Forward index explicitly enabled on SPARSE_MAP columns

```java
if (fieldSpec.getDataType() == DataType.SPARSE_MAP) {
  Preconditions.checkState(!starTreeIndexColumns.contains(column),
      "Star-tree index not supported on SPARSE_MAP column: %s", column);
  // Validate SparseMapIndexConfig keys are subset of schema sparseMapKeyTypes
}
```

**Step 2: Verify and run**

Run: `mvn compile -pl pinot-segment-local -am -DskipTests`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/utils/TableConfigUtils.java
git commit -m "feat: add SPARSE_MAP validation rules to TableConfigUtils"
```

---

## Task 25: Unit Tests — DataType and Schema

**Files:**
- Create/Modify: `pinot-spi/src/test/java/org/apache/pinot/spi/data/SparseMapFieldSpecTest.java`

**Step 1: Write tests**

Test cases:
1. `DataType.SPARSE_MAP.getStoredType()` returns `SPARSE_MAP`
2. `DataType.SPARSE_MAP.convert("{\"age\": 25}")` returns a Map
3. Schema validation accepts SPARSE_MAP as DIMENSION
4. Schema validation rejects SPARSE_MAP without sparseMapKeyTypes
5. DimensionFieldSpec JSON serialization/deserialization with sparseMapKeyTypes
6. Schema._hasSparseMapColumn flag is set correctly

**Step 2: Run tests**

Run: `mvn test -pl pinot-spi -Dtest=SparseMapFieldSpecTest`
Expected: ALL PASS

**Step 3: Commit**

```bash
git add pinot-spi/src/test/java/org/apache/pinot/spi/data/SparseMapFieldSpecTest.java
git commit -m "test: add unit tests for SPARSE_MAP DataType and schema validation"
```

---

## Task 26: Unit Tests — SparseMapIndex Creator and Reader

**Files:**
- Create: `pinot-segment-local/src/test/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexTest.java`

**Step 1: Write tests**

Test cases:
1. Create index with 3 keys (INT, STRING, DOUBLE), 100 documents, 10% fill rate per key
2. Verify presence bitmaps: correct docIds for each key
3. Verify typed reads: `getInt(docId, "age")` returns correct value
4. Verify NULL for absent docs: `getInt(missingDocId, "age")` returns default
5. Verify full map reconstruction: `getMap(docId)` returns correct key-value pairs
6. Verify inverted index: `getDocsWithKeyValue("region", "US")` returns correct docIds
7. Verify undeclared keys stored as STRING

**Step 2: Run tests**

Run: `mvn test -pl pinot-segment-local -Dtest=SparseMapIndexTest`
Expected: ALL PASS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/test/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexTest.java
git commit -m "test: add unit tests for SparseMapIndex creator and reader"
```

---

## Task 27: Unit Tests — Mutable Index

**Files:**
- Create: `pinot-segment-local/src/test/java/org/apache/pinot/segment/local/segment/index/sparsemap/MutableSparseMapIndexTest.java`

**Step 1: Write tests**

Test cases:
1. Add documents with varying keys, verify presence bitmaps
2. Concurrent reads and writes (multi-threaded test)
3. Memory tracking (bytes size increases with data)
4. getKeyDataSource returns valid DataSource
5. getMap reconstruction from mutable state

**Step 2: Run tests**

Run: `mvn test -pl pinot-segment-local -Dtest=MutableSparseMapIndexTest`
Expected: ALL PASS

**Step 3: Commit**

```bash
git add pinot-segment-local/src/test/java/org/apache/pinot/segment/local/segment/index/sparsemap/MutableSparseMapIndexTest.java
git commit -m "test: add unit tests for MutableSparseMapIndexImpl"
```

---

## Task 28: Integration Test — End-to-End

**Files:**
- Create: `pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/SparseMapIntegrationTest.java`

**Step 1: Write integration test**

Test flow:
1. Create schema with SPARSE_MAP column (`sparse_attrs` with keys: age=INT, name=STRING, score=DOUBLE)
2. Create table config with SparseMapIndex enabled (inverted index on all keys)
3. Generate test data: 1000 rows, each row has 2-5 random keys populated
4. Upload segment
5. Query: `SELECT sparse_attrs['age'] FROM table WHERE sparse_attrs['name'] = 'Alice'`
6. Query: `SELECT sparse_attrs['age'], COUNT(*) FROM table GROUP BY sparse_attrs['age']`
7. Query: `SELECT sparse_attrs FROM table LIMIT 10` (full map reconstruction)
8. Verify results are correct and typed (age returns INT, not STRING)

**Step 2: Run test**

Run: `mvn test -pl pinot-integration-tests -Dtest=SparseMapIntegrationTest`
Expected: ALL PASS

**Step 3: Commit**

```bash
git add pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/SparseMapIntegrationTest.java
git commit -m "test: add end-to-end integration test for SPARSE_MAP datatype"
```

---

## Dependency Graph

```
Task 1 (DataType enum)
  └─> Task 2 (DimensionFieldSpec)
  └─> Task 3 (Schema validation)
  └─> Task 4 (NullValuePlaceHolder)
  └─> Task 5 (ColumnDataType)
  └─> Task 6 (PinotDataType)

Task 7 (V1Constants + StandardIndexes) ─ depends on Task 8, 9, 10

Task 8 (SparseMapIndexConfig)
Task 9 (SparseMapIndexCreator interface) ─ depends on Task 1
Task 10 (SparseMapIndexReader interface) ─ depends on Task 1

Task 11 (IndexType + Plugin) ─ depends on 7, 8, 9, 10
  └─> Task 12 (OnHeapCreator) ─ depends on 9
  └─> Task 13 (ImmutableReader) ─ depends on 10
  └─> Task 14 (MutableIndex) ─ depends on 10

Task 15 (SparseMapDataSource) ─ depends on 10
Task 16 (IndexHandler) ─ depends on 11

Task 17 (StatsCollector) ─ depends on 1, 2
Task 18 (MutableSegmentImpl) ─ depends on 14, 15
Task 19 (ForwardIndexType disable) ─ depends on 1
Task 20 (DataTypeTransformerUtils) ─ depends on 6
Task 21 (SelectionOperatorUtils) ─ depends on 5
Task 22 (MapFilterOperator) ─ depends on 10, 15
Task 23 (BaseSegmentCreator) ─ depends on 2
Task 24 (TableConfigUtils) ─ depends on 1, 8

Task 25 (Unit tests: schema) ─ depends on 1-4
Task 26 (Unit tests: index) ─ depends on 12, 13
Task 27 (Unit tests: mutable) ─ depends on 14
Task 28 (Integration test) ─ depends on ALL
```

## Execution Order (Recommended)

**Wave 1 (SPI layer — no dependencies):** Tasks 1, 2, 3, 4, 8
**Wave 2 (SPI + Common):** Tasks 5, 6, 9, 10
**Wave 3 (StandardIndexes + Index Type):** Tasks 7, 11
**Wave 4 (Core implementations):** Tasks 12, 13, 14 (can be parallel)
**Wave 5 (Integration points):** Tasks 15, 16, 17, 18, 19, 20, 23
**Wave 6 (Query layer):** Tasks 21, 22
**Wave 7 (Validation):** Task 24
**Wave 8 (Tests):** Tasks 25, 26, 27, 28
