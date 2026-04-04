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
package org.apache.pinot.segment.spi.codec;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


/**
 * An immutable, ordered sequence of {@link ChunkCodec} stages that defines how chunk data is
 * encoded before being written to disk and decoded after being read back.
 *
 * <p>Invariants enforced at construction time:
 * <ul>
 *   <li>The pipeline is non-empty.</li>
 *   <li>At most one {@link ChunkCodec.CodecKind#COMPRESSOR COMPRESSOR} stage is present,
 *       and it must be the <b>last</b> stage.</li>
 *   <li>All preceding stages (if any) must be {@link ChunkCodec.CodecKind#TRANSFORM TRANSFORM}.</li>
 *   <li>Pipeline length is at most {@value #MAX_PIPELINE_LENGTH}.</li>
 * </ul>
 *
 * <p>On write the stages are applied left-to-right (transforms first, then compression).
 * On read the stages are applied right-to-left (decompress first, then reverse transforms).</p>
 */
public final class ChunkCodecPipeline {

  /** Maximum number of codec stages in a single pipeline. */
  public static final int MAX_PIPELINE_LENGTH = 8;

  /** Default pipeline: a single ZSTANDARD compressor. */
  public static final ChunkCodecPipeline DEFAULT = new ChunkCodecPipeline(
      Collections.singletonList(ChunkCodec.ZSTANDARD));

  private final List<ChunkCodec> _stages;

  /**
   * Creates a pipeline from an ordered list of codec stages.
   *
   * @throws IllegalArgumentException if invariants are violated
   */
  public ChunkCodecPipeline(List<ChunkCodec> stages) {
    Preconditions.checkArgument(stages != null && !stages.isEmpty(), "Pipeline must have at least one stage");
    Preconditions.checkArgument(stages.size() <= MAX_PIPELINE_LENGTH,
        "Pipeline length %s exceeds maximum of %s", stages.size(), MAX_PIPELINE_LENGTH);

    // Validate: all transforms first, at most one compressor at the end
    boolean seenCompressor = false;
    for (int i = 0; i < stages.size(); i++) {
      ChunkCodec codec = stages.get(i);
      Preconditions.checkArgument(codec != null, "Pipeline stage at index %s is null", i);
      if (codec.isCompressor()) {
        Preconditions.checkArgument(!seenCompressor, "Pipeline contains more than one compressor: %s", stages);
        Preconditions.checkArgument(i == stages.size() - 1,
            "Compressor %s must be the last stage in the pipeline, but found at index %s of %s",
            codec, i, stages.size());
        seenCompressor = true;
      }
    }

    _stages = Collections.unmodifiableList(new ArrayList<>(stages));
  }

  /** Returns the ordered, immutable list of codec stages. */
  public List<ChunkCodec> getStages() {
    return _stages;
  }

  /** Returns the number of stages in the pipeline. */
  public int size() {
    return _stages.size();
  }

  /** Returns the codec at the given index. */
  public ChunkCodec get(int index) {
    return _stages.get(index);
  }

  /**
   * Returns the terminal compressor, or {@link ChunkCodec#PASS_THROUGH} if the pipeline
   * contains only transforms.
   */
  public ChunkCodec getCompressor() {
    ChunkCodec last = _stages.get(_stages.size() - 1);
    return last.isCompressor() ? last : ChunkCodec.PASS_THROUGH;
  }

  /** Returns the transform stages (all stages except the terminal compressor). */
  public List<ChunkCodec> getTransforms() {
    ChunkCodec last = _stages.get(_stages.size() - 1);
    if (last.isCompressor()) {
      return _stages.subList(0, _stages.size() - 1);
    }
    return _stages;
  }

  /** Returns {@code true} if the pipeline contains any transform stages. */
  public boolean hasTransforms() {
    return !getTransforms().isEmpty();
  }

  /**
   * Maps the terminal compressor back to a {@link ChunkCompressionType} for backward
   * compatibility with readers/writers that still use the legacy enum.
   */
  public ChunkCompressionType getChunkCompressionType() {
    ChunkCodec compressor = getCompressor();
    switch (compressor) {
      case PASS_THROUGH:
        return ChunkCompressionType.PASS_THROUGH;
      case SNAPPY:
        return ChunkCompressionType.SNAPPY;
      case ZSTANDARD:
        return ChunkCompressionType.ZSTANDARD;
      case LZ4:
        return ChunkCompressionType.LZ4;
      case GZIP:
        return ChunkCompressionType.GZIP;
      case DELTA_LZ4:
        return ChunkCompressionType.DELTA;
      case DOUBLE_DELTA_LZ4:
        return ChunkCompressionType.DELTADELTA;
      default:
        throw new IllegalStateException("No ChunkCompressionType mapping for: " + compressor);
    }
  }

  /**
   * Creates a pipeline from codec names (e.g., {@code ["DELTA", "ZSTANDARD"]}).
   * Internal-only codecs (see {@link ChunkCodec#isInternalOnly()}) are rejected.
   *
   * @throws IllegalArgumentException if names contain internal-only codecs
   */
  public static ChunkCodecPipeline fromNames(List<String> names) {
    Preconditions.checkArgument(names != null && !names.isEmpty(), "Pipeline names must be non-empty");
    List<ChunkCodec> stages = new ArrayList<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      String name = names.get(i);
      Preconditions.checkArgument(name != null,
          "Pipeline codec name at index %s must be non-null", i);
      String normalized = name.trim();
      Preconditions.checkArgument(!normalized.isEmpty(),
          "Pipeline codec name at index %s must be non-blank (value: '%s')", i, name);
      ChunkCodec codec = ChunkCodec.valueOf(normalized.toUpperCase());
      codec.validateUserFacing(name);
      stages.add(codec);
    }
    return new ChunkCodecPipeline(stages);
  }

  /**
   * Creates a pipeline from on-disk int values.
   */
  public static ChunkCodecPipeline fromValues(int[] values) {
    List<ChunkCodec> stages = new ArrayList<>(values.length);
    for (int v : values) {
      stages.add(ChunkCodec.fromValue(v));
    }
    return new ChunkCodecPipeline(stages);
  }

  /**
   * Creates a single-stage pipeline from a legacy {@link ChunkCompressionType}.
   */
  public static ChunkCodecPipeline fromCompressionType(ChunkCompressionType compressionType) {
    switch (compressionType) {
      case PASS_THROUGH:
        return new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.PASS_THROUGH));
      case SNAPPY:
        return new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.SNAPPY));
      case ZSTANDARD:
        return new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.ZSTANDARD));
      case LZ4:
      case LZ4_LENGTH_PREFIXED:
        return new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.LZ4));
      case GZIP:
        return new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.GZIP));
      case DELTA:
        return new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.DELTA_LZ4));
      case DELTADELTA:
        return new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.DOUBLE_DELTA_LZ4));
      default:
        throw new IllegalArgumentException("Unsupported compression type: " + compressionType);
    }
  }

  /** Returns the pipeline as a list of codec names (for JSON serialization). */
  public List<String> toNames() {
    return _stages.stream().map(ChunkCodec::name).collect(Collectors.toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ChunkCodecPipeline)) {
      return false;
    }
    return _stages.equals(((ChunkCodecPipeline) o)._stages);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_stages);
  }

  @Override
  public String toString() {
    return _stages.stream().map(ChunkCodec::name).collect(Collectors.joining(" → ", "[", "]"));
  }
}
