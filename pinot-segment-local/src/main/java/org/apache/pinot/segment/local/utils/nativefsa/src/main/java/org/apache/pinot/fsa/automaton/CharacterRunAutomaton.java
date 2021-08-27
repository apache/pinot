package org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.automaton;

/** Automaton representation for matching char[]. */
public class CharacterRunAutomaton extends RunAutomaton {

  /**
   * Constructor specifying determinizeWorkLimit.
   *
   * @param a Automaton to match
   */
  public CharacterRunAutomaton(Automaton a) {
    super(a, true);
  }

  public boolean run(String s) {
    int p = 0;
    int l = s.length();
    int i = 0;

    int cp;
    for(boolean var5 = false; i < l; i += Character.charCount(cp)) {
      p = this.step(p, cp = s.codePointAt(i));
      if (p == -1) {
        return false;
      }
    }

    return this.accept[p];
  }

  public boolean run(char[] s, int offset, int length) {
    int p = 0;
    int l = offset + length;
    int i = offset;

    int cp;
    for(boolean var7 = false; i < l; i += Character.charCount(cp)) {
      p = this.step(p, cp = Character.codePointAt(s, i, l));
      if (p == -1) {
        return false;
      }
    }

    return this.accept[p];
  }
}
