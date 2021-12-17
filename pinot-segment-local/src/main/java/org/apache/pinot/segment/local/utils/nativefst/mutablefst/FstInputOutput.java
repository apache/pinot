/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import semiring.Semiring;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.google.common.io.Resources.asByteSource;
import static com.google.common.io.Resources.getResource;


/**
 * Handles serialization and deserialization to get it out of the FST class (not its responsibility)
 *
 * @author Atri Sharma
 */
public class FstInputOutput {

  private static final int FIRST_VERSION = 42;
  private static final int CURRENT_VERSION = 42;

  /**
   * Deserializes a symbol map from an java.io.ObjectInput
   *
   * @param in the java.io.ObjectInput. It should be already be initialized by the caller.
   * @return the deserialized symbol map
   */
  public static MutableSymbolTable readStringMap(ObjectInput in)
      throws IOException, ClassNotFoundException {

    int mapSize = in.readInt();
    MutableSymbolTable syms = new MutableSymbolTable();
    for (int i = 0; i < mapSize; i++) {
      String sym = in.readUTF();
      int index = in.readInt();
      syms.put(sym, index);
    }
    return syms;
  }

  /**
   * Deserializes an Fst from an ObjectInput
   *
   * @param in the ObjectInput. It should be already be initialized by the caller.
   */
  public static MutableFst readFstFromBinaryStream(ObjectInput in) throws IOException,
                                                                          ClassNotFoundException {
    int version = in.readInt();
    if (version < FIRST_VERSION && version > CURRENT_VERSION) {
      throw new IllegalArgumentException("cant read version fst model " + version);
    }

    MutableSymbolTable is = readStringMap(in);
    MutableSymbolTable os = readStringMap(in);

    MutableSymbolTable ss = null;
    if (in.readBoolean()) {
      ss = readStringMap(in);
    }
    return readFstWithTables(in, is, os, ss);
  }

  private static MutableFst readFstWithTables(ObjectInput in, MutableSymbolTable is, MutableSymbolTable os,
                                              MutableSymbolTable ss)
      throws IOException, ClassNotFoundException {
    int startid = in.readInt();
    Semiring semiring = (Semiring) in.readObject();
    int numStates = in.readInt();
    MutableFst res = new MutableFst(new ArrayList<MutableState>(numStates), semiring, is, os);
    if (ss != null) {
      res.useStateSymbols(ss);
    }
    for (int i = 0; i < numStates; i++) {
      int numArcs = in.readInt();
      MutableState s = new MutableState(numArcs);
      double f = in.readDouble();
      if (f == res.getSemiring().zero()) {
        f = semiring.zero();
      } else if (f == semiring.one()) {
        f = semiring.one();
      }
      s.setFinalWeight(f);
      int thisStateId = in.readInt();
      res.setState(thisStateId, s);
    }
    res.setStart(res.getState(startid));

    numStates = res.getStateCount();
    for (int i = 0; i < numStates; i++) {
      MutableState s1 = res.getState(i);
      for (int j = 0; j < s1.initialNumArcs; j++) {
        int iLabel = in.readInt();
        int oLabel = in.readInt();
        double weight = in.readDouble();
        MutableState state = res.getState(in.readInt());
        res.addArc(s1, iLabel, oLabel, state, weight);
      }
    }

    return res;
  }

  /**
   * Deserializes an Fst from disk
   *
   * @param file the binary model filename
   */
  public static MutableFst readFstFromBinaryFile(File file) {
    return loadModelFromSource(Files.asByteSource(file));
  }

  public static MutableFst readFstFromBinaryResource(String resourceName) {
    ByteSource bs = asByteSource(getResource(resourceName));
    return loadModelFromSource(bs);
  }

  private static MutableFst loadModelFromSource(ByteSource bs) {
    try (PushbackInputStream pis = new PushbackInputStream(bs.openBufferedStream(), 2)) {
      // gzip or not?
      ObjectInputStream ois;
      byte[] signature = new byte[2];
      int len = pis.read(signature); //read the signature
      pis.unread(signature, 0, len); //push back the signature to the stream
      if (signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b) {
        ois = new ObjectInputStream(new GZIPInputStream(pis));
      } else {
        ois = new ObjectInputStream(pis);
      }
      return readFstFromBinaryStream(ois);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Serializes a symbol map to an ObjectOutput
   *
   * @param map the symbol map to serialize
   * @param out the ObjectOutput. It should be already be initialized by the caller.
   */
  private static void writeStringMap(SymbolTable map, ObjectOutput out)
      throws IOException {
    out.writeInt(map.size());
    for (ObjectIntCursor<String> cursor : map) {
      out.writeUTF(cursor.key);
      out.writeInt(cursor.value);
    }
  }

  /**
   * Serializes the current Fst instance to an ObjectOutput
   *
   * @param out the ObjectOutput. It should be already be initialized by the caller.
   */
  public static void writeFstToBinaryStream(Fst fst, ObjectOutput out) throws IOException {
    out.writeInt(CURRENT_VERSION);
    writeStringMap(fst.getInputSymbols(), out);
    writeStringMap(fst.getOutputSymbols(), out);
    out.writeBoolean(fst.isUsingStateSymbols()); // whether or not we used a state symbol table
    if (fst.isUsingStateSymbols()) {
      writeStringMap(fst.getStateSymbols(), out);
    }
    out.writeInt(fst.getStartState().getId());

    out.writeObject(fst.getSemiring());
    out.writeInt(fst.getStateCount());

    Map<State, Integer> stateMap = new IdentityHashMap<>(fst.getStateCount());
    for (int i = 0; i < fst.getStateCount(); i++) {
      State s = fst.getState(i);
      out.writeInt(s.getArcCount());
      out.writeDouble(s.getFinalWeight());
      out.writeInt(s.getId());
      stateMap.put(s, i);
    }

    int numStates = fst.getStateCount();
    for (int i = 0; i < numStates; i++) {
      State s = fst.getState(i);
      int numArcs = s.getArcCount();
      for (int j = 0; j < numArcs; j++) {
        Arc a = s.getArc(j);
        out.writeInt(a.getIlabel());
        out.writeInt(a.getOlabel());
        out.writeDouble(a.getWeight());
        out.writeInt(stateMap.get(a.getNextState()));
      }
    }
  }

  public static void writeFstToBinaryFile(Fst fst, File file) throws IOException {
    ByteSink bs = Files.asByteSink(file);
    try (ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(bs.openBufferedStream()))) {
      writeFstToBinaryStream(fst, oos);
    }
  }
}
