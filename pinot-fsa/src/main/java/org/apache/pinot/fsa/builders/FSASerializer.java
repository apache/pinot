package org.apache.pinot.fsa.builders;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import org.apache.pinot.fsa.FSA;
import org.apache.pinot.fsa.FSAFlags;


/**
 * All FSA serializers (to binary formats) will implement this interface.
 */
public interface FSASerializer {
  /**
   * Serialize a finite state automaton to an output stream.
   * 
   * @param fsa The automaton to serialize.
   * @param os The output stream to serialize to.
   * @param <T> A subclass of {@link OutputStream}, returned for chaining. 
   * @return Returns <code>T</code> for chaining.
   * @throws IOException Rethrown if an I/O error occurs.
   */
  public <T extends OutputStream> T serialize(FSA fsa, T os) throws IOException;

  /**
   * @return Returns the set of flags supported by the serializer (and the output
   * automaton).
   */
  public Set<FSAFlags> getFlags();

  /**
   * Sets the filler separator (only if {@link #getFlags()} returns
   * {@link FSAFlags#SEPARATORS}).
   * 
   * @param filler The filler separator byte.
   * @return Returns <code>this</code> for call chaining.
   */
  public FSASerializer withFiller(byte filler);

  /**
   * Sets the annotation separator (only if {@link #getFlags()} returns
   * {@link FSAFlags#SEPARATORS}).
   * 
   * @param annotationSeparator The filler separator byte.
   * @return Returns <code>this</code> for call chaining.
   */
  public FSASerializer withAnnotationSeparator(byte annotationSeparator);

  /**
   * Enables support for right language count on nodes, speeding up perfect hash
   * counts (only if {@link #getFlags()} returns {@link FSAFlags#NUMBERS}).
   * 
   * @return Returns <code>this</code> for call chaining.
   */
  public FSASerializer withNumbers();
}
