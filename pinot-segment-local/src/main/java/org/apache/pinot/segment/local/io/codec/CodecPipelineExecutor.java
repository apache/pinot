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
/// Executors are immutable and thread-safe, so lifecycle owners can retain and share them;
/// concurrent calls must use separate caller-owned workspaces and output buffers.
///
/// The executor is codec-agnostic: it holds an ordered list of internal bound stages, each pairing
/// a handler with its parsed options. The registry and handlers are a closed, package-private
/// runtime; this class is the only public entry point and only drives the pipeline loop.
///
/// ### Buffer contract
///
/// - [#encode]: reads the source's remaining bytes and returns a scratch-owned readable view,
///       invalidated by the next encode call with that workspace or by closing the workspace.
/// - [#decode]: reads the source's remaining bytes into a caller-owned destination and leaves
///       the destination ready for read. Source and destination must not alias scratch storage
///       or each other. Source consumption is permitted on decode.
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

    void encode(ByteBuffer src, ByteBuffer dst) throws IOException {
      _handler.encode(_options, _ctx, src, dst);
    }

    void decode(ByteBuffer src, ByteBuffer dst) throws IOException {
      _handler.decode(_options, _ctx, src, dst);
    }

    int maxEncodedSize(int inputSize) {
      return _handler.maxEncodedSize(_options, _ctx, inputSize);
    }

    boolean requiresDirectDecodeDstBuffer() {
      return _handler.requiresDirectDecodeDstBuffer();
    }

    boolean isCompression() {
      return _handler.kind() == CodecKind.COMPRESSION;
    }

    String canonicalize() {
      return _handler.canonicalize(_options);
    }
  }

  /// Shared bounded-buffer machinery for encode and decode scratch spaces. Every stage checkout
  /// resets byte order and limits capacity to the validated bound, including after backing-buffer
  /// growth invalidates cached views.
  private static final class StageViewWorkspace implements AutoCloseable {
    private final String _ownerName;
    private final ByteBuffer[] _buffers = new ByteBuffer[2];
    private final ByteBuffer[][] _stageViews = {new ByteBuffer[0], new ByteBuffer[0]};
    private final int[][] _stageViewCapacities = {new int[0], new int[0]};
    private int[] _stageBounds = new int[0];
    private int _allocationCount;
    private int _viewCreationCount;
    private boolean _closed;

    private StageViewWorkspace(String ownerName) {
      _ownerName = ownerName;
    }

    private ByteBuffer buffer(int stageIndex, int slot, int capacity) {
      ensureOpen();
      Preconditions.checkArgument(capacity >= 0, "Scratch capacity must be non-negative: %s", capacity);
      ensureCapacity(slot, capacity);
      ensureStageViewCapacity(slot, stageIndex + 1);
      // Limit the returned view's capacity to this stage's validated bound. Reusing a larger
      // backing buffer must not let a corrupt inner frame exploit capacity left from an earlier
      // larger chunk.
      ByteBuffer view = _stageViews[slot][stageIndex];
      if (view == null || _stageViewCapacities[slot][stageIndex] != capacity) {
        view = _buffers[slot].duplicate().order(ByteOrder.BIG_ENDIAN);
        view.clear();
        view.limit(capacity);
        view = view.slice().order(ByteOrder.BIG_ENDIAN);
        _stageViews[slot][stageIndex] = view;
        _stageViewCapacities[slot][stageIndex] = capacity;
        _viewCreationCount++;
      } else {
        view.clear();
        view.order(ByteOrder.BIG_ENDIAN);
      }
      return view;
    }

    private void ensureCapacity(int slot, int capacity) {
      ensureOpen();
      ByteBuffer buffer = _buffers[slot];
      if (buffer == null || buffer.capacity() < capacity) {
        CleanerUtil.cleanQuietly(buffer);
        _buffers[slot] = ByteBuffer.allocateDirect(capacity);
        _stageViews[slot] = new ByteBuffer[0];
        _stageViewCapacities[slot] = new int[0];
        _allocationCount++;
      }
    }

    private void ensureStageViewCapacity(int slot, int requiredSize) {
      if (_stageViews[slot].length >= requiredSize) {
        return;
      }
      ByteBuffer[] views = new ByteBuffer[requiredSize];
      System.arraycopy(_stageViews[slot], 0, views, 0, _stageViews[slot].length);
      _stageViews[slot] = views;
      int[] capacities = new int[requiredSize];
      System.arraycopy(_stageViewCapacities[slot], 0, capacities, 0, _stageViewCapacities[slot].length);
      _stageViewCapacities[slot] = capacities;
    }

    private int[] stageBounds(int stageCount) {
      ensureOpen();
      if (_stageBounds.length < stageCount) {
        _stageBounds = new int[stageCount];
      }
      return _stageBounds;
    }

    private void ensureOpen() {
      Preconditions.checkState(!_closed, "%s is closed", _ownerName);
    }

    private boolean isClosed() {
      return _closed;
    }

    private int allocationCount() {
      return _allocationCount;
    }

    private int viewCreationCount() {
      return _viewCreationCount;
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
        _stageViews[i] = new ByteBuffer[0];
        _stageViewCapacities[i] = new int[0];
      }
      _stageBounds = new int[0];
    }
  }

  /// Caller-owned, reusable encode workspace.
  ///
  /// The workspace retains one source view and at most two direct buffers, growing the buffers on
  /// demand. Pipeline stages alternate between capacity-bounded views of those buffers, so repeated
  /// chunk writes do not allocate and explicitly clean direct buffers per stage. This class is not
  /// thread-safe; each writer or calling thread must own a separate instance. Any view returned by
  /// [#encode(ByteBuffer, int, long, EncodeScratch)] is owned by this workspace and is invalidated
  /// by the next encode call or by [#close()].
  public static final class EncodeScratch implements AutoCloseable {
    private final StageViewWorkspace _workspace = new StageViewWorkspace("EncodeScratch");
    private ByteBuffer _source;
    private ByteBuffer _sourceView;
    private int _sourceViewCreationCount;

    private ByteBuffer buffer(int stageIndex, int slot, int capacity) {
      return _workspace.buffer(stageIndex, slot, capacity);
    }

    private ByteBuffer sourceView(ByteBuffer source) {
      ensureOpen();
      if (_source != source) {
        _source = source;
        _sourceView = source.duplicate();
        _sourceViewCreationCount++;
      }
      _sourceView.limit(source.limit());
      _sourceView.position(source.position());
      _sourceView.order(ByteOrder.BIG_ENDIAN);
      return _sourceView;
    }

    private void prepare(int[] stageBounds, int stageCount) {
      ensureOpen();
      int evenCapacity = 0;
      int oddCapacity = 0;
      for (int i = 0; i < stageCount; i++) {
        if ((i & 1) == 0) {
          evenCapacity = Math.max(evenCapacity, stageBounds[i]);
        } else {
          oddCapacity = Math.max(oddCapacity, stageBounds[i]);
        }
      }
      _workspace.ensureCapacity(0, evenCapacity);
      if (stageCount > 1) {
        _workspace.ensureCapacity(1, oddCapacity);
      }
    }

    private int[] stageBounds(int stageCount) {
      return _workspace.stageBounds(stageCount);
    }

    private void ensureOpen() {
      _workspace.ensureOpen();
    }

    int allocationCount() {
      return _workspace.allocationCount();
    }

    int viewCreationCount() {
      return _workspace.viewCreationCount() + _sourceViewCreationCount;
    }

    @Override
    public void close() {
      if (_workspace.isClosed()) {
        return;
      }
      _workspace.close();
      _source = null;
      _sourceView = null;
    }
  }

  /// Caller-owned, reusable decode workspace.
  ///
  /// The workspace retains at most two direct buffers and grows them on demand. Multi-stage
  /// decoding alternates between those buffers, so repeated chunk reads avoid allocating and
  /// explicitly cleaning a direct buffer for every intermediate stage. This class is not
  /// thread-safe; each reader context or calling thread must own a separate instance.
  public static final class DecodeScratch implements AutoCloseable {
    private final StageViewWorkspace _workspace = new StageViewWorkspace("DecodeScratch");
    private ByteBuffer _finalDestination;
    private ByteBuffer _finalView;

    private ByteBuffer buffer(int stageIndex, int slot, int capacity) {
      return _workspace.buffer(stageIndex, slot, capacity);
    }

    private ByteBuffer finalOutput(ByteBuffer destination) {
      ensureOpen();
      if (_finalDestination != destination) {
        _finalDestination = destination;
        _finalView = destination.duplicate().order(ByteOrder.BIG_ENDIAN);
      }
      _finalView.clear();
      _finalView.order(ByteOrder.BIG_ENDIAN);
      return _finalView;
    }

    private int[] stageBounds(int stageCount) {
      return _workspace.stageBounds(stageCount);
    }

    private void ensureOpen() {
      _workspace.ensureOpen();
    }

    int allocationCount() {
      return _workspace.allocationCount();
    }

    int viewCreationCount() {
      return _workspace.viewCreationCount();
    }

    @Override
    public void close() {
      if (_workspace.isClosed()) {
        return;
      }
      _workspace.close();
      _finalDestination = null;
      _finalView = null;
    }
  }

  private final List<BoundStage<?>> _stages;
  private final String _canonicalSpec;
  private final DataType _storedType;
  private final boolean _hasCompression;
  private final boolean _requiresDirectDecodeDstBuffer;

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
    _stages = List.copyOf(stages);
    _canonicalSpec = buildCanonical(stages);
    _storedType = ctx.getDataType();
    _hasCompression = stages.stream().anyMatch(BoundStage::isCompression);
    // decode() writes the final output through stage zero. All other stage outputs use
    // caller-owned direct scratch buffers, so only stage zero constrains the caller's dst.
    _requiresDirectDecodeDstBuffer = stages.get(0).requiresDirectDecodeDstBuffer();
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
    return encodedStageBounds(decodedSize, maxStageSize, maxCumulativeSize, null);
  }

  private int encodedStageBounds(int decodedSize, int maxStageSize, long maxCumulativeSize,
      int[] stageBounds) {
    Preconditions.checkArgument(decodedSize >= 0, "decodedSize must be non-negative: %s", decodedSize);
    Preconditions.checkArgument(maxStageSize >= decodedSize,
        "maxStageSize %s must be at least decodedSize %s", maxStageSize, decodedSize);
    Preconditions.checkArgument(maxCumulativeSize >= decodedSize,
        "maxCumulativeSize %s must be at least decodedSize %s", maxCumulativeSize, decodedSize);
    Preconditions.checkArgument(stageBounds == null || stageBounds.length >= _stages.size(),
        "stageBounds length must be at least %s", _stages.size());
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
      if (stageBounds != null) {
        stageBounds[i] = size;
      }
    }
    return size;
  }

  /// Encodes through capacity-bounded caller-owned scratch buffers.
  ///
  /// Every stage output is written directly into one of two alternating direct buffers. The
  /// returned readable view is owned by `scratch`: callers must consume or copy it before the next
  /// call using that workspace, and must not use it after the workspace is closed. The caller's
  /// source position, limit, and byte order are not modified. Typed input is interpreted in
  /// persisted big-endian order, independent of the caller view's byte order. The source must
  /// not alias this workspace's buffers (including a view returned by a previous call).
  ///
  /// @param src               decoded chunk data, ready for read
  /// @param maxStageSize      maximum permitted output bound for any stage
  /// @param maxCumulativeSize maximum permitted sum of all stage-output bounds
  /// @param scratch           caller-owned workspace, not shared across concurrent calls
  /// @return scratch-owned encoded bytes ready for read
  public ByteBuffer encode(ByteBuffer src, int maxStageSize, long maxCumulativeSize, EncodeScratch scratch)
      throws IOException {
    scratch.ensureOpen();
    int[] stageBounds = scratch.stageBounds(_stages.size());
    encodedStageBounds(src.remaining(), maxStageSize, maxCumulativeSize, stageBounds);
    scratch.prepare(stageBounds, _stages.size());
    ByteBuffer current = scratch.sourceView(src);
    for (int i = 0; i < _stages.size(); i++) {
      ByteBuffer output = scratch.buffer(i, i & 1, stageBounds[i]);
      _stages.get(i).encode(current, output);
      Preconditions.checkState(output.position() == 0,
          "Codec stage %s did not return an encoded buffer ready for read", i);
      Preconditions.checkState(output.remaining() <= stageBounds[i],
          "Codec stage %s encoded size %s exceeds validated bound %s", i, output.remaining(), stageBounds[i]);
      current = output;
    }
    return current;
  }

  /// Decodes with caller-owned reusable scratch buffers.
  ///
  /// The caller must not share scratch across concurrent decode calls and must close it when the
  /// owning reader context is closed. Source and destination must not alias each other or scratch
  /// storage. The source may be consumed; destination position and limit are overwritten, leaving
  /// it ready for read, and its byte order is preserved.
  ///
  /// @param src                 encoded chunk data, ready for read
  /// @param dst                 caller-owned output; direct when required by the first codec
  /// @param expectedDecodedSize exact decoded bytes validated by the caller, not an inner codec frame
  /// @param maxStageSize      maximum permitted stage-output bound
  /// @param maxCumulativeSize maximum permitted sum of stage-output bounds
  /// @param scratch           caller-owned workspace, not shared across concurrent calls
  public void decode(ByteBuffer src, ByteBuffer dst, int expectedDecodedSize, int maxStageSize,
      long maxCumulativeSize, DecodeScratch scratch)
      throws IOException {
    scratch.ensureOpen();
    Preconditions.checkArgument(!_requiresDirectDecodeDstBuffer || dst.isDirect(),
        "decode(src, dst) requires a direct ByteBuffer for pipeline: %s", _canonicalSpec);
    Preconditions.checkArgument(expectedDecodedSize >= 0 && expectedDecodedSize <= dst.capacity(),
        "expectedDecodedSize %s is out of range [0, %s]", expectedDecodedSize, dst.capacity());
    Preconditions.checkArgument(maxStageSize >= expectedDecodedSize,
        "maxStageSize %s must be at least expectedDecodedSize %s", maxStageSize, expectedDecodedSize);
    Preconditions.checkArgument(maxCumulativeSize >= expectedDecodedSize,
        "maxCumulativeSize %s must be at least expectedDecodedSize %s", maxCumulativeSize, expectedDecodedSize);

    int stageCount = _stages.size();
    int[] maxOutputAfterStage = scratch.stageBounds(stageCount);
    int maxEncodedSize = encodedStageBounds(expectedDecodedSize, maxStageSize, maxCumulativeSize,
        maxOutputAfterStage);
    Preconditions.checkArgument(src.remaining() <= maxEncodedSize,
        "Encoded input size %s exceeds composed pipeline bound %s for expected decoded size %s",
        src.remaining(), maxEncodedSize, expectedDecodedSize);

    // Decode every stage into its destination. Intermediate stages alternate between two reusable
    // direct buffers whose views are capacity-limited to the forward maxEncodedSize chain rooted
    // at the validated final decoded size. A corrupt inner frame-controlled length can therefore
    // only trigger a bounded "exceeds dst capacity" failure, never a frame-controlled allocation.
    ByteBuffer finalOutput = scratch.finalOutput(dst);
    ByteOrder sourceOrder = src.order();
    try {
      src.order(ByteOrder.BIG_ENDIAN);
      ByteBuffer current = src;
      int scratchSlot = 0;
      for (int i = stageCount - 1; i >= 0; i--) {
        ByteBuffer output;
        if (i == 0) {
          output = finalOutput;
        } else {
          output = scratch.buffer(i - 1, scratchSlot, maxOutputAfterStage[i - 1]);
          scratchSlot ^= 1;
        }
        _stages.get(i).decode(current, output);
        current = output;
      }
    } finally {
      src.order(sourceOrder);
    }
    if (finalOutput.remaining() != expectedDecodedSize) {
      throw new IOException("Codec pipeline decoded " + finalOutput.remaining() + " bytes but expected "
          + expectedDecodedSize + " for pipeline " + _canonicalSpec + ". Segment may be corrupt.");
    }
    // decode() flips the final view. Mirror that readable range onto the caller's buffer while
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
