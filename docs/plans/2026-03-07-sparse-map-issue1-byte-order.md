# Sparse Map Issue 1: Standardize Binary Format Byte Order

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Standardize the SparseMap index binary format to BIG_ENDIAN throughout and add a magic-byte prefix + version field for runtime format validation.

**Architecture:** The key metadata section in `OnHeapSparseMapIndexCreator` is written in LITTLE_ENDIAN (via raw byte helpers `writeInt`/`writeLong` that shift LSB-first), while the header and all other sections use BIG_ENDIAN (`DataOutputStream`). The fix rewrites the two byte-order helpers to be MSB-first, adds a 4-byte MAGIC constant at the start of the header, bumps the version to 2, and updates `ImmutableSparseMapIndexReader` to validate the magic bytes and read key metadata as BIG_ENDIAN.

**Tech Stack:** Java 11, Apache Pinot, JUnit 5, Maven (./mvnw)

---

### Task 1: Fix `writeInt` and `writeLong` to BIG_ENDIAN in `OnHeapSparseMapIndexCreator`

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/OnHeapSparseMapIndexCreator.java`

**Context:**
The current helpers (lines 258–269) write LITTLE_ENDIAN (LSB at lowest address):
```java
private static void writeInt(byte[] buf, int offset, int value) {
  buf[offset]     = (byte) (value);
  buf[offset + 1] = (byte) (value >> 8);
  buf[offset + 2] = (byte) (value >> 16);
  buf[offset + 3] = (byte) (value >> 24);
}
private static void writeLong(byte[] buf, int offset, long value) {
  for (int i = 0; i < 8; i++) {
    buf[offset + i] = (byte) (value >> (i * 8));
  }
}
```
Fix them to BIG_ENDIAN (MSB at lowest address) to match `DataOutputStream` convention used everywhere else.

**Step 1: Replace `writeInt` helper**

In `OnHeapSparseMapIndexCreator.java`, replace:
```java
  private static void writeInt(byte[] buf, int offset, int value) {
    buf[offset] = (byte) (value);
    buf[offset + 1] = (byte) (value >> 8);
    buf[offset + 2] = (byte) (value >> 16);
    buf[offset + 3] = (byte) (value >> 24);
  }
```
With:
```java
  private static void writeInt(byte[] buf, int offset, int value) {
    buf[offset]     = (byte) (value >> 24);
    buf[offset + 1] = (byte) (value >> 16);
    buf[offset + 2] = (byte) (value >> 8);
    buf[offset + 3] = (byte) (value);
  }
```

**Step 2: Replace `writeLong` helper**

Replace:
```java
  private static void writeLong(byte[] buf, int offset, long value) {
    for (int i = 0; i < 8; i++) {
      buf[offset + i] = (byte) (value >> (i * 8));
    }
  }
```
With:
```java
  private static void writeLong(byte[] buf, int offset, long value) {
    for (int i = 0; i < 8; i++) {
      buf[offset + i] = (byte) (value >> ((7 - i) * 8));
    }
  }
```

---

### Task 2: Add MAGIC constant and update header write/read

**Files:**
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/OnHeapSparseMapIndexCreator.java`
- Modify: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/ImmutableSparseMapIndexReader.java`

**Context:**
The current 64-byte header layout is:
```
[0..3]   int VERSION=1
[4..7]   int numKeys
[8..11]  int numDocs
[12..19] long keyDictOffset
[20..27] long keyMetaOffset
[28..35] long perKeyDataOffset
[36..63] 28 bytes of zero padding
```
We prepend a 4-byte MAGIC (`0x53504D58` = ASCII "SPMX") before VERSION and bump VERSION to 2.
New layout (still 64 bytes total):
```
[0..3]   int MAGIC=0x53504D58
[4..7]   int VERSION=2
[8..11]  int numKeys
[12..15] int numDocs
[16..23] long keyDictOffset
[24..31] long keyMetaOffset
[32..39] long perKeyDataOffset
[40..63] 24 bytes of zero padding
```

**Step 1: Update constants and `writeHeader` in `OnHeapSparseMapIndexCreator`**

Change:
```java
  private static final int VERSION = 1;
  private static final int HEADER_SIZE = 64;
```
To:
```java
  private static final int MAGIC = 0x53504D58;   // "SPMX"
  private static final int VERSION = 2;
  private static final int HEADER_SIZE = 64;
```

Replace `writeHeader` method body:
```java
  private void writeHeader(DataOutputStream dos, int numKeys, long keyDictOffset, long keyMetaOffset, long perKeyOffset)
      throws IOException {
    dos.writeInt(VERSION);
    dos.writeInt(numKeys);
    dos.writeInt(_numDocs);
    dos.writeLong(keyDictOffset);
    dos.writeLong(keyMetaOffset);
    dos.writeLong(perKeyOffset);
    for (int i = 0; i < HEADER_SIZE - 4 * 3 - 8 * 3; i++) {
      dos.write(0);
    }
  }
```
With:
```java
  private void writeHeader(DataOutputStream dos, int numKeys, long keyDictOffset, long keyMetaOffset, long perKeyOffset)
      throws IOException {
    dos.writeInt(MAGIC);
    dos.writeInt(VERSION);
    dos.writeInt(numKeys);
    dos.writeInt(_numDocs);
    dos.writeLong(keyDictOffset);
    dos.writeLong(keyMetaOffset);
    dos.writeLong(perKeyOffset);
    // 64 - (4 ints * 4 bytes) - (3 longs * 8 bytes) = 64 - 16 - 24 = 24 bytes padding
    for (int i = 0; i < 24; i++) {
      dos.write(0);
    }
  }
```

**Step 2: Update `ImmutableSparseMapIndexReader` header parsing**

Add a `MAGIC` constant and validate it. Change:
```java
  private static final int HEADER_SIZE = 64;
  private static final int KEY_METADATA_ENTRY_SIZE = 53;
```
To:
```java
  private static final int MAGIC = 0x53504D58;   // "SPMX"
  private static final int HEADER_SIZE = 64;
  private static final int KEY_METADATA_ENTRY_SIZE = 53;
```

In the constructor, replace the header parsing block:
```java
    // ---- Parse Header (big-endian via DataOutputStream in creator) ----
    // The creator uses DataOutputStream for the header, which writes big-endian.
    // PinotDataBuffer reads in native order, so we wrap bytes to force big-endian.
    byte[] headerBytes = new byte[HEADER_SIZE];
    dataBuffer.copyTo(0, headerBytes, 0, HEADER_SIZE);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
    /* int version = */ headerBuf.getInt();
    _numKeys = headerBuf.getInt();
    _numDocs = headerBuf.getInt();
    long keyDictOffset = headerBuf.getLong();
    long keyMetaOffset = headerBuf.getLong();
    _perKeyDataSectionOffset = headerBuf.getLong();
```
With:
```java
    // ---- Parse Header (BIG_ENDIAN throughout) ----
    byte[] headerBytes = new byte[HEADER_SIZE];
    dataBuffer.copyTo(0, headerBytes, 0, HEADER_SIZE);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
    int magic = headerBuf.getInt();
    if (magic != MAGIC) {
      throw new IOException(
          String.format("Invalid SparseMap index: expected magic 0x%08X but got 0x%08X", MAGIC, magic));
    }
    int version = headerBuf.getInt();
    if (version != 2) {
      throw new IOException("Unsupported SparseMap index version: " + version + " (expected 2)");
    }
    _numKeys = headerBuf.getInt();
    _numDocs = headerBuf.getInt();
    long keyDictOffset = headerBuf.getLong();
    long keyMetaOffset = headerBuf.getLong();
    _perKeyDataSectionOffset = headerBuf.getLong();
```

**Step 3: Fix key metadata parsing to BIG_ENDIAN**

In `ImmutableSparseMapIndexReader` constructor, change:
```java
    ByteBuffer metaBuf = ByteBuffer.wrap(metaBlock).order(ByteOrder.LITTLE_ENDIAN);
```
To:
```java
    ByteBuffer metaBuf = ByteBuffer.wrap(metaBlock).order(ByteOrder.BIG_ENDIAN);
```

Also remove the misleading comment above it:
```java
    // ---- Parse Key Metadata (little-endian, 53 bytes per key) ----
```
Replace with:
```java
    // ---- Parse Key Metadata (BIG_ENDIAN, 53 bytes per key) ----
```

And update the Javadoc comment at the class level that says "little-endian":
```java
 *   <li>Key metadata (53 bytes/key, little-endian): storedTypeOrdinal(byte), numDocs(int),
```
→
```java
 *   <li>Key metadata (53 bytes/key, big-endian): storedTypeOrdinal(byte), numDocs(int),
```

---

### Task 3: Write failing tests that exercise the byte-order contract

**Files:**
- Modify: `pinot-segment-local/src/test/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexTest.java`

**Step 1: Add test for magic byte validation**

Add this test to `SparseMapIndexTest.java`:
```java
@Test
public void testInvalidMagicBytesThrows() throws Exception {
  // Write a valid index
  File indexDir = _tmpDir;  // use existing @TempDir or create one
  // ... build minimal index ...
  // Corrupt the first 4 bytes (magic)
  File indexFile = new File(indexDir, "col" + V1Constants.Indexes.SPARSE_MAP_INDEX_FILE_EXTENSION);
  try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
    raf.seek(0);
    raf.writeInt(0xDEADBEEF);   // bad magic
  }
  PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
  assertThrows(IOException.class, () -> new ImmutableSparseMapIndexReader(buf, null));
}
```

**Step 2: Add round-trip test asserting BIG_ENDIAN key metadata is read correctly**

Add a test that writes a multi-key index with known INT and LONG values, reads it back, and asserts exact values (this will fail before the fix because the reader will mis-read LITTLE_ENDIAN data as BIG_ENDIAN):
```java
@Test
public void testByteOrderRoundTripAllTypes() throws Exception {
  // Write index with INT, LONG, FLOAT, DOUBLE keys
  // Read back each key's value from specific doc
  // Assert exact values match what was written
  // (Implicitly validates that byte order is consistent end-to-end)
}
```
(Exact implementation follows the existing pattern in `SparseMapIndexTest.java`.)

**Step 3: Run tests to confirm they FAIL before the fix**

```bash
cd /home/user/pinot
./mvnw test -pl pinot-segment-local \
  -Dtest=SparseMapIndexTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
```
Expected: tests fail (byte-order mismatch).

---

### Task 4: Apply fixes and verify tests pass

**Step 1: Apply all code changes from Tasks 1 and 2**

Make the edits described in Tasks 1 and 2 in order.

**Step 2: Run the full SparseMap test suite**

```bash
cd /home/user/pinot
./mvnw test -pl pinot-segment-local \
  -Dtest=SparseMapIndexTest,SparseMapIndexEndToEndTest,SparseMapSegmentCreationTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
```
Expected: ALL tests pass.

**Step 3: Apply code style**

```bash
cd /home/user/pinot
./mvnw spotless:apply checkstyle:check \
  -pl pinot-segment-local \
  -Dlicense.skip
```

**Step 4: Commit**

```bash
cd /home/user/pinot
git add pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/OnHeapSparseMapIndexCreator.java \
        pinot-segment-local/src/main/java/org/apache/pinot/segment/local/segment/index/sparsemap/ImmutableSparseMapIndexReader.java \
        pinot-segment-local/src/test/java/org/apache/pinot/segment/local/segment/index/sparsemap/SparseMapIndexTest.java
git commit -m "fix(sparsemap): standardize binary format to BIG_ENDIAN, add magic+version validation

- Fix writeInt/writeLong helpers in OnHeapSparseMapIndexCreator to use
  BIG_ENDIAN (MSB-first) instead of LITTLE_ENDIAN, consistent with
  DataOutputStream used for all other sections
- Add MAGIC=0x53504D58 (SPMX) as first 4 bytes of header; bump VERSION to 2
- Update ImmutableSparseMapIndexReader to validate magic bytes and version
  on open, and read key metadata section as BIG_ENDIAN
- Add unit tests for magic validation and full round-trip byte order check"
```

---

## Summary of Changes

| File | Change |
|------|--------|
| `OnHeapSparseMapIndexCreator.java` | Fix `writeInt`/`writeLong` to BIG_ENDIAN; add `MAGIC`; bump `VERSION` to 2; update `writeHeader` |
| `ImmutableSparseMapIndexReader.java` | Add `MAGIC`; validate magic+version on open; read key metadata as `BIG_ENDIAN`; update Javadoc |
| `SparseMapIndexTest.java` | Add tests for magic validation and byte-order round-trip |
