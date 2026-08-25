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
package org.apache.pinot.segment.local.io.codec;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.codec.CodecInvocation;
import org.apache.pinot.segment.spi.codec.CodecPipeline;
import org.apache.pinot.segment.spi.codec.CodecSpecParser;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Executes a parsed and validated [CodecPipeline] for a single forward-index chunk.
///
/// Write path: values → transforms (in order) → compression stages → bytes stored on disk.
/// Read path: bytes from disk → reverse compression/transform stages → values.
///
/// The executor is constructed once per column from the canonical `codecSpec` string
/// stored in the file header and is thread-safe for concurrent read calls (it holds no mutable
/// per-call state).
///
/// The executor is codec-agnostic: it holds an ordered list of internal bound stages, each pairing
/// a handler with its parsed options. The registry and handlers are a closed, package-private
/// runtime; this class is the only public entry point and only drives the pipeline loop.
///
/// ### Buffer contract
///
/// - [#encode]: `src` is ready for read (position=0); returns a new
///       [ByteBuffer] ready for read containing the encoded bytes.
/// - [#decode(ByteBuffer)]: `src` is ready for read; returns a new
///       [ByteBuffer] ready for read containing the decoded bytes.
/// - [#maxEncodedSize]: returns an upper bound on encoded size.
public final class CodecPipelineExecutor {

  /// A codec handler bound to the options parsed from a specific pipeline invocation.
  private static final class BoundStage<O extends CodecOptions> {
    final ChunkCodecHandler<O> _handler;
    final O _options;
    final CodecContext _ctx;

    BoundStage(ChunkCodecHandler<O> handler, O options, CodecContext ctx) {
      _handler = handler;
      _options = options;
      _ctx = ctx;
    }

    ByteBuffer encode(ByteBuffer src) throws IOException {
      return _handler.encode(_options, _ctx, src);
    }

    ByteBuffer decode(ByteBuffer src) throws IOException {
      return _handler.decode(_options, _ctx, src);
    }

    void decodeInto(ByteBuffer src, ByteBuffer dst) throws IOException {
      _handler.decodeInto(_options, _ctx, src, dst);
    }

    int maxEncodedSize(int inputSize) {
      return _handler.maxEncodedSize(_options, inputSize);
    }

    boolean requiresDirectDstBuffer() {
      return _handler.requiresDirectDstBuffer();
    }

    boolean isCompression() {
      return _handler.kind() == CodecKind.COMPRESSION;
    }

    String canonicalize() {
      return _handler.canonicalize(_options);
    }
  }

  /// Caller-owned, reusable decode workspace.
  ///
  /// The workspace retains at most two direct buffers and grows them on demand. Multi-stage
  /// decoding alternates between those buffers, so repeated chunk reads avoid allocating and
  /// explicitly cleaning a direct buffer for every intermediate stage. This class is not
  /// thread-safe; each reader context or calling thread must own a separate instance.
  public static final class DecodeScratch implements AutoCloseable {
    private final ByteBuffer[] _buffers = new ByteBuffer[2];
    private int[] _stageBounds = new int[0];
    private int _allocationCount;
    private boolean _closed;

    private ByteBuffer buffer(int slot, int capacity) {
      ensureOpen();
      Preconditions.checkArgument(capacity >= 0, "Scratch capacity must be non-negative: %s", capacity);
      ByteBuffer buffer = _buffers[slot];
      if (buffer == null || buffer.capacity() < capacity) {
        CleanerUtil.cleanQuietly(buffer);
        buffer = ByteBuffer.allocateDirect(capacity);
        _buffers[slot] = buffer;
        _allocationCount++;
      }
      // Limit the returned view's capacity to this stage's validated bound. Reusing a larger
      // backing buffer must not let a corrupt inner frame exploit capacity left from an earlier
      // larger chunk.
      ByteBuffer view = buffer.duplicate();
      view.clear();
      view.limit(capacity);
      return view.slice();
    }

    private int[] stageBounds(int stageCount) {
      ensureOpen();
      if (_stageBounds.length < stageCount) {
        _stageBounds = new int[stageCount];
      }
      return _stageBounds;
    }

    private void ensureOpen() {
      Preconditions.checkState(!_closed, "DecodeScratch is closed");
    }

    int allocationCount() {
      return _allocationCount;
    }

    @Override
    public void close() {
      if (_closed) {
        return;
      }
      _closed = true;
      for (int i = 0; i < _buffers.length; i++) {
        CleanerUtil.cleanQuietly(_buffers[i]);
        _buffers[i] = null;
      }
      _stageBounds = new int[0];
    }
  }

  private final List<BoundStage<?>> _stages;
  private final String _canonicalSpec;
  private final DataType _storedType;
  private final boolean _hasCompression;
  private final boolean _requiresDirectDstBuffer;

  /// Creates an executor by parsing and validating the given spec.
  ///
  /// @param spec       the codec DSL string (e.g. `"LZ4,ZSTD(3)"`)
  /// @param storedType stored data type used for validation
  public static CodecPipelineExecutor create(String spec, DataType storedType) {
    return create(spec, new CodecContext(storedType), CodecRegistry.DEFAULT);
  }

  /// Package-scoped construction hook for tests that provide a custom closed registry.
  static CodecPipelineExecutor create(String spec, CodecContext ctx, CodecRegistry registry) {
    CodecPipeline pipeline = CodecSpecParser.parse(spec);
    CodecPipelineValidator.validate(pipeline, registry, ctx);
    return new CodecPipelineExecutor(pipeline, registry, ctx);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private CodecPipelineExecutor(CodecPipeline pipeline, CodecRegistry registry, CodecContext ctx) {
    List<CodecInvocation> invocations = pipeline.stages();
    if (invocations.isEmpty()) {
      throw new IllegalArgumentException("Codec pipeline must contain at least one stage");
    }
    List<BoundStage<?>> stages = new ArrayList<>(invocations.size());

    for (CodecInvocation inv : invocations) {
      ChunkCodecHandler handler = (ChunkCodecHandler) registry.getOrThrow(inv.name());
      CodecOptions opts = handler.parseOptions(inv.args());
      stages.add(new BoundStage<>(handler, opts, ctx));
    }
    _stages = stages;
    _canonicalSpec = buildCanonical(stages);
    _storedType = ctx.getDataType();
    _hasCompression = stages.stream().anyMatch(BoundStage::isCompression);
    // decodeInto() writes the final output through stage zero. All other stage outputs use
    // executor-owned direct scratch buffers, so only stage zero constrains the caller's dst.
    _requiresDirectDstBuffer = stages.get(0).requiresDirectDstBuffer();
  }

  /// Returns the canonical spec string derived from the parsed pipeline.
  public String getCanonicalSpec() {
    return _canonicalSpec;
  }

  /// Returns the stored column type to which this executor was validated and bound.
  public DataType getStoredType() {
    return _storedType;
  }

  /// Returns an upper bound on the number of bytes that [#encode] may produce for
  /// a decoded chunk of the given byte length.
  int maxEncodedSize(int decodedSize) {
    return maxEncodedSize(decodedSize, Integer.MAX_VALUE, Long.MAX_VALUE);
  }

  /// Returns the composed encoded-size bound while requiring every stage's bound to stay within
  /// `maxStageSize`. This lets an on-disk format reject a pipeline/chunk-size combination before
  /// either the writer produces an unreadable file or the reader allocates excessive scratch.
  int maxEncodedSize(int decodedSize, int maxStageSize) {
    return maxEncodedSize(decodedSize, maxStageSize, Long.MAX_VALUE);
  }

  /// Returns the composed encoded-size bound while also capping the sum of all stage-output
  /// bounds. The cumulative limit bounds CPU and allocation churn for long pipelines even when
  /// each individual stage stays below `maxStageSize`.
  public int maxEncodedSize(int decodedSize, int maxStageSize, long maxCumulativeSize) {
    Preconditions.checkArgument(decodedSize >= 0, "decodedSize must be non-negative: %s", decodedSize);
    Preconditions.checkArgument(maxStageSize >= decodedSize,
        "maxStageSize %s must be at least decodedSize %s", maxStageSize, decodedSize);
    Preconditions.checkArgument(maxCumulativeSize >= decodedSize,
        "maxCumulativeSize %s must be at least decodedSize %s", maxCumulativeSize, decodedSize);
    int size = decodedSize;
    long cumulativeSize = 0;
    for (int i = 0; i < _stages.size(); i++) {
      size = _stages.get(i).maxEncodedSize(size);
      if (size < 0 || size > maxStageSize) {
        throw new IllegalArgumentException(
            "Codec stage " + i + " maximum encoded size " + size + " is outside [0, " + maxStageSize
                + "] for pipeline " + _canonicalSpec);
      }
      cumulativeSize += size;
      if (cumulativeSize > maxCumulativeSize) {
        throw new IllegalArgumentException(
            "Codec pipeline cumulative stage-output bound " + cumulativeSize + " exceeds " + maxCumulativeSize
                + " at stage " + i + " for pipeline " + _canonicalSpec);
      }
    }
    return size;
  }

  /// Encodes `src` through the pipeline and returns the encoded bytes ready for read.
  ///
  /// The readable range is interpreted in the persisted big-endian typed-value order, independent
  /// of the caller view's byte order. The caller's position, limit, and order are not modified.
  ///
  /// @param src decoded chunk data, ready for read (position=0, limit=dataSize)
  /// @return encoded buffer ready for read; the caller owns this buffer
  public ByteBuffer encode(ByteBuffer src) throws IOException {
    ByteBuffer input = src.duplicate().order(ByteOrder.BIG_ENDIAN);
    ByteBuffer current = input;
    try {
      for (BoundStage<?> stage : _stages) {
        ByteBuffer previous = current;
        current = stage.encode(previous);
        if (previous != input && previous != current) {
          CleanerUtil.cleanQuietly(previous);
        }
      }
      return current;
    } catch (IOException | RuntimeException | Error e) {
      if (current != input) {
        CleanerUtil.cleanQuietly(current);
      }
      throw e;
    }
  }

  /// Decodes `src` through the reversed pipeline and returns the original bytes.
  ///
  /// @param src encoded chunk data, ready for read
  /// @return decoded buffer ready for read; the caller owns this buffer
  ByteBuffer decode(ByteBuffer src) throws IOException {
    ByteBuffer current = src;
    try {
      for (int i = _stages.size() - 1; i >= 0; i--) {
        ByteBuffer previous = current;
        current = _stages.get(i).decode(previous);
        if (previous != src && previous != current) {
          CleanerUtil.cleanQuietly(previous);
        }
      }
      return current;
    } catch (IOException | RuntimeException | Error e) {
      if (current != src) {
        CleanerUtil.cleanQuietly(current);
      }
      throw e;
    }
  }

  /// Decodes `src` through the reversed pipeline, writing the result directly into
  /// `dst`.  On return `dst` is flipped and ready for read (position=0,
  /// limit=decoded size).
  ///
  /// For single-stage pipelines (transform-only or compression-only), the decoded bytes are
  /// written directly into `dst`, avoiding an intermediate allocation.  For multi-stage
  /// pipelines an intermediate buffer is allocated for any stage that is not the last in the
  /// reversed pipeline; the final stage writes directly into `dst`.
  ///
  /// @param src encoded chunk data, ready for read
  /// @param dst caller-supplied output buffer; must be a *direct* [ByteBuffer] when
  ///            the pipeline requires it (see [ChunkCodecHandler#requiresDirectDstBuffer]);
  ///            must have sufficient [ByteBuffer#capacity()] for the decoded data; its
  ///            position and limit are overwritten before returning
  /// @throws IOException              if decoding fails
  /// @throws IllegalArgumentException if `dst` is not direct when required, or does not have
  ///                                  enough capacity
  void decode(ByteBuffer src, ByteBuffer dst) throws IOException {
    decode(src, dst, dst.capacity());
  }

  /// Decodes into `dst` while bounding every intermediate allocation from the expected final
  /// decoded size. This is the segment-reader entry point: codec frames are untrusted and may
  /// advertise arbitrary decoded lengths, so an intermediate stage must never allocate directly
  /// from its frame header.
  ///
  /// @param src                 encoded chunk data, ready for read
  /// @param dst                 caller-owned final output buffer
  /// @param expectedDecodedSize exact decoded bytes declared and validated by the outer format
  void decode(ByteBuffer src, ByteBuffer dst, int expectedDecodedSize) throws IOException {
    decode(src, dst, expectedDecodedSize, Integer.MAX_VALUE, Long.MAX_VALUE);
  }

  /// Variant of [#decode(ByteBuffer, ByteBuffer, int)] that caps every intermediate scratch
  /// buffer. Callers reading untrusted persisted data should pass the format's validated limit.
  void decode(ByteBuffer src, ByteBuffer dst, int expectedDecodedSize, int maxIntermediateSize)
      throws IOException {
    decode(src, dst, expectedDecodedSize, maxIntermediateSize, Long.MAX_VALUE);
  }

  /// Variant that also caps the sum of stage-output bounds, limiting total work and allocation
  /// churn for an otherwise-valid long pipeline. This convenience overload owns a temporary
  /// workspace; production chunk readers should use the caller-owned [DecodeScratch] overload.
  void decode(ByteBuffer src, ByteBuffer dst, int expectedDecodedSize, int maxIntermediateSize,
      long maxCumulativeSize)
      throws IOException {
    try (DecodeScratch scratch = new DecodeScratch()) {
      decode(src, dst, expectedDecodedSize, maxIntermediateSize, maxCumulativeSize, scratch);
    }
  }

  /// Decodes with caller-owned reusable scratch buffers.
  ///
  /// The caller must not share scratch across concurrent decode calls and must close it when the
  /// owning reader context is closed.
  public void decode(ByteBuffer src, ByteBuffer dst, int expectedDecodedSize, int maxIntermediateSize,
      long maxCumulativeSize, DecodeScratch scratch)
      throws IOException {
    scratch.ensureOpen();
    Preconditions.checkArgument(!_requiresDirectDstBuffer || dst.isDirect(),
        "decode(src, dst) requires a direct ByteBuffer for pipeline: %s", _canonicalSpec);
    Preconditions.checkArgument(expectedDecodedSize >= 0 && expectedDecodedSize <= dst.capacity(),
        "expectedDecodedSize %s is out of range [0, %s]", expectedDecodedSize, dst.capacity());
    Preconditions.checkArgument(maxIntermediateSize >= expectedDecodedSize,
        "maxIntermediateSize %s must be at least expectedDecodedSize %s", maxIntermediateSize, expectedDecodedSize);
    Preconditions.checkArgument(maxCumulativeSize >= expectedDecodedSize,
        "maxCumulativeSize %s must be at least expectedDecodedSize %s", maxCumulativeSize, expectedDecodedSize);

    int stageCount = _stages.size();
    int[] maxOutputAfterStage = scratch.stageBounds(stageCount);
    int maxSize = expectedDecodedSize;
    long cumulativeSize = 0;
    for (int i = 0; i < stageCount; i++) {
      maxSize = _stages.get(i).maxEncodedSize(maxSize);
      if (maxSize < 0 || maxSize > maxIntermediateSize) {
        throw new IllegalArgumentException(
            "Codec stage " + i + " maximum encoded size " + maxSize + " is outside [0, "
                + maxIntermediateSize + "] for pipeline " + _canonicalSpec);
      }
      cumulativeSize += maxSize;
      if (cumulativeSize > maxCumulativeSize) {
        throw new IllegalArgumentException(
            "Codec pipeline cumulative stage-output bound " + cumulativeSize + " exceeds " + maxCumulativeSize
                + " at stage " + i + " for pipeline " + _canonicalSpec);
      }
      maxOutputAfterStage[i] = maxSize;
    }
    Preconditions.checkArgument(src.remaining() <= maxOutputAfterStage[stageCount - 1],
        "Encoded input size %s exceeds composed pipeline bound %s for expected decoded size %s",
        src.remaining(), maxOutputAfterStage[stageCount - 1], expectedDecodedSize);

    // Decode every stage through decodeInto(). Intermediate stages alternate between two reusable
    // direct buffers whose views are capacity-limited to the forward maxEncodedSize chain rooted
    // at the validated final decoded size. A corrupt inner frame-controlled length can therefore
    // only trigger a bounded "exceeds dst capacity" failure, never a frame-controlled allocation.
    ByteBuffer finalOutput = dst.duplicate().order(ByteOrder.BIG_ENDIAN);
    ByteBuffer current = src;
    int scratchSlot = 0;
    for (int i = stageCount - 1; i >= 0; i--) {
      ByteBuffer output;
      if (i == 0) {
        output = finalOutput;
      } else {
        output = scratch.buffer(scratchSlot, maxOutputAfterStage[i - 1]);
        scratchSlot ^= 1;
      }
      _stages.get(i).decodeInto(current, output);
      current = output;
    }
    if (finalOutput.remaining() != expectedDecodedSize) {
      throw new IOException("Codec pipeline decoded " + finalOutput.remaining() + " bytes but expected "
          + expectedDecodedSize + " for pipeline " + _canonicalSpec + ". Segment may be corrupt.");
    }
    // decodeInto() flips the final view. Mirror that readable range onto the caller's buffer while
    // preserving its independently configured byte order.
    dst.clear();
    dst.limit(finalOutput.limit());
    dst.position(finalOutput.position());
  }

  /// Returns true if the pipeline has at least one compression stage.
  boolean isCompressed() {
    return _hasCompression;
  }

  // -------------------------------------------------------------------------
  // Canonical spec builder
  // -------------------------------------------------------------------------

  private static String buildCanonical(List<BoundStage<?>> stages) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < stages.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(stages.get(i).canonicalize());
    }
    return sb.toString();
  }
}
