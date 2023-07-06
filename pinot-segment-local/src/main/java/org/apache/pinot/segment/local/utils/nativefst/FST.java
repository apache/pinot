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
package org.apache.pinot.segment.local.utils.nativefst;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTSerializerImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;


/**
 * This is a top abstract class for handling finite state automata. These
 * automata are arc-based, a design described in Jan Daciuk's <i>Incremental
 * Construction of Finite-State Automata and Transducers, and Their Use in the
 * Natural Language Processing</i> (PhD thesis, Technical University of Gdansk).
 */
public abstract class FST implements Iterable<ByteBuffer> {
  /**
   * @param in The input stream.
   * @param length Length of input to be read
   * @return Reads remaining bytes upto length from an input stream and returns
   * them as a byte array. Null if no data was read
   * @throws IOException Rethrown if an I/O exception occurs.
   */
  protected static byte[] readRemaining(InputStream in, int length)
      throws IOException {
    byte[] buf = new byte[length];
    int readLen;

    readLen = in.read(buf, 0, length);

    if (readLen == -1) {
      return null;
    }

    return buf;
  }

  //HACK: atri
  public static FST read(InputStream stream)
          throws IOException {
    return read(stream, false, new DirectMemoryManager(FST.class.getName()), 0);
  }

  /**
   * Wrapper for the main read function
   */
  public static FST read(InputStream stream, final int fstDataSize)
      throws IOException {
    return read(stream, false, new DirectMemoryManager(FST.class.getName()), fstDataSize);
  }

  //Hack: Atri
  public static FST read(InputStream stream, boolean hasOutputSymbols, PinotDataBufferMemoryManager memoryManager)
          throws IOException {
    return read(stream, hasOutputSymbols, memoryManager, 0);
  }

  /**
   * A factory for reading automata in any of the supported versions.
   *
   * @param stream
   *          The input stream to read automaton data from. The stream is not
   *          closed.
   * @return Returns an instantiated automaton. Never null.
   * @throws IOException
   *           If the input stream does not represent an automaton or is
   *           otherwise invalid.
   */
  public static FST read(InputStream stream, boolean hasOutputSymbols, PinotDataBufferMemoryManager memoryManager, final int fstDataSize)
      throws IOException {
    FSTHeader header = FSTHeader.read(stream);

    switch (header._version) {
      case ImmutableFST.VERSION:
        return new ImmutableFST(stream, hasOutputSymbols, memoryManager, fstDataSize);
      default:
        throw new IOException(
            String.format(Locale.ROOT, "Unsupported automaton version: 0x%02x", header._version & 0xFF));
    }
  }

  public static <T extends FST> T read(InputStream stream, Class<? extends T> clazz, boolean hasOutputSymbolse)
          throws IOException {
    return read(stream, clazz, hasOutputSymbolse, 0);
  }

  /**
   * A factory for reading a specific FST subclass, including proper casting.
   *
   * @param stream
   *          The input stream to read automaton data from. The stream is not
   *          closed.
   * @param clazz A subclass of {@link FST} to cast the read automaton to.
   * @param <T> A subclass of {@link FST} to cast the read automaton to.
   * @return Returns an instantiated automaton. Never null.
   * @throws IOException
   *           If the input stream does not represent an automaton, is otherwise
   *           invalid or the class of the automaton read from the input stream
   *           is not assignable to <code>clazz</code>.
   */
  public static <T extends FST> T read(InputStream stream, Class<? extends T> clazz, boolean hasOutputSymbols, final int fstDataSize)
      throws IOException {
    FST fst = read(stream, hasOutputSymbols, new DirectMemoryManager(FST.class.getName()), fstDataSize);
    if (!clazz.isInstance(fst)) {
      throw new IOException(
          String.format(Locale.ROOT, "Expected FST type %s, but read an incompatible type %s.", clazz.getName(),
              fst.getClass().getName()));
    }
    return clazz.cast(fst);
  }

  /**
   *  Print to String
   */
  public static String printToString(final FST fst) {
    StringBuilder b = new StringBuilder();

    b.append("initial state: ").append(fst.getRootNode()).append("\n");

    fst.visitInPreOrder(state -> {
      b.append("state : " + state).append("\n");
      for (int arc = fst.getFirstArc(state); arc != 0; arc = fst.getNextArc(arc)) {
        b.append(
            " { arc: " + arc + " targetNode: " + (fst.isArcFinal(arc) ? "final arc" : fst.getEndNode(arc)) + " label: "
                + (char) fst.getArcLabel(arc) + " }");
      }

      b.append("\n");
      return true;
    });

    return b.toString();
  }

  /**
   * @return Returns the identifier of the root node of this automaton. Returns
   *         0 if the start node is also the end node (the automaton is empty).
   */
  public abstract int getRootNode();

  /**
   * @param node
   *          Identifier of the node.
   * @return Returns the identifier of the first arc leaving <code>node</code>
   *         or 0 if the node has no outgoing arcs.
   */
  public abstract int getFirstArc(int node);

  /**
   * @param arc
   *          The arc's identifier.
   * @return Returns the identifier of the next arc after <code>arc</code> and
   *         leaving <code>node</code>. Zero is returned if no more arcs are
   *         available for the node.
   */
  public abstract int getNextArc(int arc);

  /**
   * @param node
   *          Identifier of the node.
   * @param label
   *          The arc's label.
   * @return Returns the identifier of an arc leaving <code>node</code> and
   *         labeled with <code>label</code>. An identifier equal to 0 means the
   *         node has no outgoing arc labeled <code>label</code>.
   */
  public abstract int getArc(int node, byte label);

  /**
   * @param arc
   *          The arc's identifier.
   * @return Return the label associated with a given <code>arc</code>.
   */
  public abstract byte getArcLabel(int arc);

  /**
   * Get output symbol for the given arc
   * @param arc Arc for which the output symbol is requested
   * @return Output symbol, null if not present
   */
  public abstract int getOutputSymbol(int arc);

  /**
   * @param arc
   *          The arc's identifier.
   * @return Returns <code>true</code> if the destination node at the end of
   *         this <code>arc</code> corresponds to an input sequence created when
   *         building this automaton.
   */
  public abstract boolean isArcFinal(int arc);

  /**
   * @param arc
   *          The arc's identifier.
   * @return Returns <code>true</code> if this <code>arc</code> does not have a
   *         terminating node (@link {@link #getEndNode(int)} will throw an
   *         exception). Implies {@link #isArcFinal(int)}.
   */
  public abstract boolean isArcTerminal(int arc);

  /**
   * @param arc
   *          The arc's identifier.
   * @return Return the end node pointed to by a given <code>arc</code>.
   *         Terminal arcs (those that point to a terminal state) have no end
   *         node representation and throw a runtime exception.
   */
  public abstract int getEndNode(int arc);

  /**
   * @return Returns a set of flags for this FST instance.
   */
  public abstract Set<FSTFlags> getFlags();

  /**
   * @param node
   *          Identifier of the node.
   *
   * @return Returns the number of sequences reachable from the given state if
   *         the automaton was compiled with {@link FSTFlags#NUMBERS}. The size
   *         of the right language of the state, in other words.
   *
   * @throws UnsupportedOperationException
   *           If the automaton was not compiled with {@link FSTFlags#NUMBERS}.
   *           The value can then be computed by manual count of
   *           {@link #getSequences}.
   */
  public int getRightLanguageCount(int node) {
    throw new UnsupportedOperationException("Automaton not compiled with " + FSTFlags.NUMBERS);
  }

  /**
   * Returns an iterator over all binary sequences starting at the given FST
   * state (node) and ending in final nodes. This corresponds to a set of
   * suffixes of a given prefix from all sequences stored in the automaton.
   *
   * <p>
   * The returned iterator is a {@link ByteBuffer} whose contents changes on
   * each call to {@link Iterator#next()}. The keep the contents between calls
   * to {@link Iterator#next()}, one must copy the buffer to some other
   * location.
   * </p>
   *
   * <p>
   * <b>Important.</b> It is guaranteed that the returned byte buffer is backed
   * by a byte array and that the content of the byte buffer starts at the
   * array's index 0.
   * </p>
   *
   * @param node
   *          Identifier of the starting node from which to return subsequences.
   * @return An iterable over all sequences encoded starting at the given node.
   */
  public Iterable<ByteBuffer> getSequences(final int node) {
    if (node == 0) {
      return Collections.emptyList();
    }

    return () -> new ByteSequenceIterator(FST.this, node);
  }

  /**
   * An alias of calling {@link #iterator} directly ({@link FST} is also
   * {@link Iterable}).
   *
   * @return Returns all sequences encoded in the automaton.
   */
  public final Iterable<ByteBuffer> getSequences() {
    return getSequences(getRootNode());
  }

  /**
   * Returns an iterator over all binary sequences starting from the initial FST
   * state (node) and ending in final nodes. The returned iterator is a
   * {@link ByteBuffer} whose contents changes on each call to
   * {@link Iterator#next()}. The keep the contents between calls to
   * {@link Iterator#next()}, one must copy the buffer to some other location.
   *
   * <p>
   * <b>Important.</b> It is guaranteed that the returned byte buffer is backed
   * by a byte array and that the content of the byte buffer starts at the
   * array's index 0.
   * </p>
   */
  public final Iterator<ByteBuffer> iterator() {
    return getSequences().iterator();
  }

  /**
   * Same as {@link #visitInPostOrder(StateVisitor, int)}, starting from root
   * automaton node.
   *
   * @param v Visitor to receive traversal calls.
   * @param <T> A subclass of {@link StateVisitor}.
   * @return Returns the argument (for access to anonymous class fields).
   */
  public <T extends StateVisitor> T visitInPostOrder(T v) {
    return visitInPostOrder(v, getRootNode());
  }

  /**
   * Visits all states reachable from <code>node</code> in postorder. Returning
   * false from {@link StateVisitor#accept(int)} immediately terminates the
   * traversal.
   *
   * @param v Visitor to receive traversal calls.
   * @param <T> A subclass of {@link StateVisitor}.
   * @param node Identifier of the node.
   * @return Returns the argument (for access to anonymous class fields).
   */
  public <T extends StateVisitor> T visitInPostOrder(T v, int node) {
    visitInPostOrder(v, node, new BitSet());
    return v;
  }

  /** Private recursion. */
  private boolean visitInPostOrder(StateVisitor v, int node, BitSet visited) {
    if (visited.get(node)) {
      return true;
    }
    visited.set(node);

    for (int arc = getFirstArc(node); arc != 0; arc = getNextArc(arc)) {
      if (!isArcTerminal(arc)) {
        if (!visitInPostOrder(v, getEndNode(arc), visited)) {
          return false;
        }
      }
    }

    return v.accept(node);
  }

  /**
   * Same as {@link #visitInPreOrder(StateVisitor, int)}, starting from root
   * automaton node.
   *
   * @param v Visitor to receive traversal calls.
   * @param <T> A subclass of {@link StateVisitor}.
   * @return Returns the argument (for access to anonymous class fields).
   */
  public <T extends StateVisitor> T visitInPreOrder(T v) {
    return visitInPreOrder(v, getRootNode());
  }

  /**
   * Visits all states in preorder. Returning false from
   * {@link StateVisitor#accept(int)} skips traversal of all sub-states of a
   * given state.
   *
   * @param v Visitor to receive traversal calls.
   * @param <T> A subclass of {@link StateVisitor}.
   * @param node Identifier of the node.
   * @return Returns the argument (for access to anonymous class fields).
   */
  public <T extends StateVisitor> T visitInPreOrder(T v, int node) {
    visitInPreOrder(v, node, new BitSet());
    return v;
  }

  /**
   * Build a map from a serialized string
   *
   * @param inputString Serialized string
   * @return
   */
  protected Map<Integer, Integer> buildMap(String inputString) {
    Map<Integer, Integer> hashMap = new HashMap<>();

    inputString = inputString.substring(1, inputString.length() - 1);

    if (inputString.isEmpty()) {
      return hashMap;
    }

    String[] pairs = inputString.split(",");

    for (String pair : pairs) {

      String[] keyVal = pair.split("=");

      int key = Integer.parseInt(keyVal[0].trim());
      int val = Integer.parseInt(keyVal[1].trim());

      hashMap.put(key, val);
    }

    return hashMap;
  }

  public abstract boolean isArcLast(int arc);

  public int save(FileOutputStream fileOutputStream) {
    try {
      final byte[] fsaData =
          new FSTSerializerImpl().withNumbers().serialize(this, new ByteArrayOutputStream()).toByteArray();

      fileOutputStream.write(fsaData);

      return fsaData.length;
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /** Private recursion. */
  private void visitInPreOrder(StateVisitor v, int node, BitSet visited) {
    if (visited.get(node)) {
      return;
    }
    visited.set(node);

    if (v.accept(node)) {
      for (int arc = getFirstArc(node); arc != 0; arc = getNextArc(arc)) {
        if (!isArcTerminal(arc)) {
          visitInPreOrder(v, getEndNode(arc), visited);
        }
      }
    }
  }

  /**
   * Returns a string representation of this automaton.
   */
  @Override
  public String toString() {
    return printToString(this);
  }
}
