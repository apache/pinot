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
package org.apache.pinot.fsa;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

/**
 * This is a top abstract class for handling finite state automata. These
 * automata are arc-based, a design described in Jan Daciuk's <i>Incremental
 * Construction of Finite-State Automata and Transducers, and Their Use in the
 * Natural Language Processing</i> (PhD thesis, Technical University of Gdansk).
 */
public abstract class FSA implements Iterable<ByteBuffer> {
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
   * @return Returns a set of flags for this FSA instance.
   */
  public abstract Set<FSAFlags> getFlags();

  /**
   * @param node
   *          Identifier of the node.
   * @return Calculates and returns the number of arcs of a given node.
   */
  public int getArcCount(int node) {
    int count = 0;
    for (int arc = getFirstArc(node); arc != 0; arc = getNextArc(arc)) {
      count++;
    }
    return count;
  }

  /**
   * @param node
   *          Identifier of the node.
   * 
   * @return Returns the number of sequences reachable from the given state if
   *         the automaton was compiled with {@link FSAFlags#NUMBERS}. The size
   *         of the right language of the state, in other words.
   * 
   * @throws UnsupportedOperationException
   *           If the automaton was not compiled with {@link FSAFlags#NUMBERS}.
   *           The value can then be computed by manual count of
   *           {@link #getSequences}.
   */
  public int getRightLanguageCount(int node) {
    throw new UnsupportedOperationException("Automaton not compiled with " + FSAFlags.NUMBERS);
  }

  /**
   * Returns an iterator over all binary sequences starting at the given FSA
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
      return Collections.<ByteBuffer> emptyList();
    }

    return new Iterable<ByteBuffer>() {
      public Iterator<ByteBuffer> iterator() {
        return new ByteSequenceIterator(FSA.this, node);
      }
    };
  }

  /**
   * An alias of calling {@link #iterator} directly ({@link FSA} is also
   * {@link Iterable}).
   * 
   * @return Returns all sequences encoded in the automaton.
   */
  public final Iterable<ByteBuffer> getSequences() {
    return getSequences(getRootNode());
  }

  /**
   * Returns an iterator over all binary sequences starting from the initial FSA
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
   * Visit all states. The order of visiting is undefined. This method may be
   * faster than traversing the automaton in post or preorder since it can scan
   * states linearly. Returning false from {@link StateVisitor#accept(int)}
   * immediately terminates the traversal.
   * 
   * @param v Visitor to receive traversal calls.
   * @param <T> A subclass of {@link StateVisitor}. 
   * @return Returns the argument (for access to anonymous class fields).
   */
  public <T extends StateVisitor> T visitAllStates(T v) {
    return visitInPostOrder(v);
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
    if (visited.get(node))
      return true;
    visited.set(node);

    for (int arc = getFirstArc(node); arc != 0; arc = getNextArc(arc)) {
      if (!isArcTerminal(arc)) {
        if (!visitInPostOrder(v, getEndNode(arc), visited))
          return false;
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
   * @param in The input stream. 
   * @return Reads all remaining bytes from an input stream and returns
   * them as a byte array. 
   * @throws IOException Rethrown if an I/O exception occurs.
   */
  protected static final byte[] readRemaining(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024 * 8];
    int len;
    while ((len = in.read(buffer)) >= 0) {
      baos.write(buffer, 0, len);
    }
    return baos.toByteArray();
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
  public static FSA read(InputStream stream) throws IOException {
    final FSAHeader header = FSAHeader.read(stream);

    switch (header.version) {
      case FSA5.VERSION:
        return new FSA5(stream);
      case CFSA.VERSION:
        return new CFSA(stream);
      case CFSA2.VERSION:
        return new CFSA2(stream);
      default:
        throw new IOException(
            String.format(Locale.ROOT, "Unsupported automaton version: 0x%02x", header.version & 0xFF));
    }
  }

  /**
   * A factory for reading a specific FSA subclass, including proper casting.
   * 
   * @param stream
   *          The input stream to read automaton data from. The stream is not
   *          closed.
   * @param clazz A subclass of {@link FSA} to cast the read automaton to.
   * @param <T> A subclass of {@link FSA} to cast the read automaton to.
   * @return Returns an instantiated automaton. Never null.
   * @throws IOException
   *           If the input stream does not represent an automaton, is otherwise
   *           invalid or the class of the automaton read from the input stream
   *           is not assignable to <code>clazz</code>.
   */
  public static <T extends FSA> T read(InputStream stream, Class<? extends T> clazz) throws IOException {
    FSA fsa = read(stream);
    if (!clazz.isInstance(fsa)) {
      throw new IOException(String.format(Locale.ROOT, "Expected FSA type %s, but read an incompatible type %s.",
          clazz.getName(), fsa.getClass().getName()));
    }
    return clazz.cast(fsa);
  }
}
