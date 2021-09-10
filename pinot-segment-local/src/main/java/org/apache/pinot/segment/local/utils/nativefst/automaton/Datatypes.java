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

package org.apache.pinot.segment.local.utils.nativefst.automaton;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


/**
 * Basic automata for representing common datatypes
 * related to Unicode, XML, and XML Schema.
 */
public final class Datatypes {

  private static final Map<String, Automaton> _automata;

  private static final Automaton _ws;

  private static final Set<String> _unicodeBlockNames;

  private static final Set<String> _unicodeCategoryNames;

  private static final Set<String> _xmlNames;

  private static final String[] _unicodeBlockNamesArray =
      {"BasicLatin", "Latin-1Supplement", "LatinExtended-A", "LatinExtended-B",
          "IPAExtensions", "SpacingModifierLetters", "CombiningDiacriticalMarks", "Greek",
          "Cyrillic", "Armenian", "Hebrew", "Arabic", "Syriac", "Thaana", "Devanagari",
          "Bengali", "Gurmukhi", "Gujarati", "Oriya", "Tamil", "Telugu", "Kannada",
          "Malayalam", "Sinhala", "Thai", "Lao", "Tibetan", "Myanmar", "Georgian",
          "HangulJamo", "Ethiopic", "Cherokee", "UnifiedCanadianAboriginalSyllabics",
          "Ogham", "Runic", "Khmer", "Mongolian", "LatinExtendedAdditional", "GreekExtended",
          "GeneralPunctuation", "SuperscriptsandSubscripts", "CurrencySymbols",
          "CombiningMarksforSymbols", "LetterlikeSymbols", "NumberForms", "Arrows",
          "MathematicalOperators", "MiscellaneousTechnical", "ControlPictures", "OpticalCharacterRecognition", "EnclosedAlphanumerics", "BoxDrawing", "BlockElements",
          "GeometricShapes", "MiscellaneousSymbols", "Dingbats", "BraillePatterns",
          "CJKRadicalsSupplement", "KangxiRadicals", "IdeographicDescriptionCharacters", "CJKSymbolsandPunctuation", "Hiragana", "Katakana", "Bopomofo",
          "HangulCompatibilityJamo", "Kanbun", "BopomofoExtended", "EnclosedCJKLettersandMonths",
          "CJKCompatibility", "CJKUnifiedIdeographsExtensionA", "CJKUnifiedIdeographs", "YiSyllables",
          "YiRadicals", "HangulSyllables", "CJKCompatibilityIdeographs", "AlphabeticPresentationForms", "ArabicPresentationForms-A",
          "CombiningHalfMarks", "CJKCompatibilityForms", "SmallFormVariants",
          "ArabicPresentationForms-B", "Specials", "HalfwidthandFullwidthForms",
          "Specials", "OldItalic", "Gothic", "Deseret", "ByzantineMusicalSymbols",
          "MusicalSymbols", "MathematicalAlphanumericSymbols",
          "CJKUnifiedIdeographsExtensionB", "CJKCompatibilityIdeographsSupplement", "Tags"};

  private static final String[] _unicodeCategoryNamesArray =
      {"Lu", "Ll", "Lt", "Lm", "Lo", "L", "Mn", "Mc", "Me", "M", "Nd", "Nl",
          "No", "N", "Pc", "Pd", "Ps", "Pe", "Pi", "Pf", "Po", "P", "Zs", "Zl", "Zp",
          "Z", "Sm", "Sc", "Sk", "So", "S", "Cc", "Cf", "Co", "Cn", "C"};

  private static final String[] _xmlNamesArray =
      {"NCName", "QName", "Char", "NameChar", "URI", "anyname", "noap", "whitespace",
          "whitespacechar", "string", "boolean", "decimal", "float", "integer", "duration",
          "dateTime", "time", "date", "gYearMonth", "gYear", "gMonthDay", "gDay", "hexBinary",
          "base64Binary", "NCName2", "NCNames", "QName2", "Nmtoken2", "Nmtokens", "Name2",
          "Names", "language"};

  private Datatypes() {
  }

  /**
   * Invoke during compilation to pre-build automata.
   * (Default: <tt>build</tt>, relative to the current working directory.)
   */
  public static void main(String[] args) {
    long t = System.currentTimeMillis();
    boolean b = Automaton.setAllowMutate(true);
    buildAll();
    Automaton.setAllowMutate(b);

    for (Map.Entry<String, Automaton> e : _automata.entrySet()) {
      store(e.getKey(), e.getValue());
    }
    System.out.println("Time for building automata: " + (System.currentTimeMillis() - t) + "ms");
  }

  /**
   * Returns pre-built automaton.
   * Automata are loaded as resources from the class loader of the <tt>Datatypes</tt> class.
   * (Typically, the pre-built automata are stored in the same jar as this class.)
   * <p>Loaded automata are cached in memory.
   * @param name name of automaton
   * @return automaton
   */
  public static Automaton get(String name) {
    Automaton a = _automata.get(name);
    if (a == null) {
      a = load(name);
      _automata.put(name, a);
    }
    return a;
  }

  /**
   * Checks whether the given string is the name of a Unicode block (see {@link #get(String)}).
   */
  public static boolean isUnicodeBlockName(String name) {
    return _unicodeBlockNames.contains(name);
  }

  /**
   * Checks whether the given string is the name of a Unicode category (see {@link #get(String)}).
   */
  public static boolean isUnicodeCategoryName(String name) {
    return _unicodeCategoryNames.contains(name);
  }

  /**
   * Checks whether the given string is the name of an XML / XML Schema automaton (see {@link #get(String)}).
   */
  public static boolean isXMLName(String name) {
    return _xmlNames.contains(name);
  }

  /**
   * Checks whether a given automaton is available.
   * @param name automaton name
   * @return true if the automaton is available
   */
  public static boolean exists(String name) {
    try {
      //noinspection ConstantConditions
      Datatypes.class.getClassLoader().getResource(name + ".aut").openStream().close();
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  private static Automaton load(String name) {
    try {
      URL url = Datatypes.class.getClassLoader().getResource(name + ".aut");
      //noinspection ConstantConditions
      return Automaton.load(url.openStream());
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static void store(String name, Automaton a) {
    String dir = System.getProperty("pinot.fsa.store");
    if (dir == null) {
      dir = "build";
    }
    try {
      a.store((new FileOutputStream(dir + "/" + name + ".aut")));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void buildAll() {
    String[] xmlexps =
        {"Extender", "[\u3031-\u3035\u309D-\u309E\u30FC-\u30FE\u00B7\u02D0\u02D1\u0387\u0640" +
            "\u0E46\u0EC6\u3005]", "CombiningChar", "[\u0300-\u0345\u0360-\u0361\u0483-\u0486" +
            "\u0591-\u05A1\u05A3-\u05B9\u05BB-\u05BD\u05C1-\u05C2\u064B-\u0652"
                + "\u06D6-\u06DC\u06DD-\u06DF\u06E0-\u06E4\u06E7-\u06E8\u06EA-\u06ED\u0901-\u0903" +
            "\u093E-\u094C\u0951-\u0954" + "\u0962-\u0963\u0981-\u0983\u09C0-\u09C4" +
            "\u09C7-\u09C8\u09CB-\u09CD\u09E2-\u09E3\u0A40-\u0A42\u0A47-\u0A48"
                + "\u0A4B-\u0A4D\u0A70-\u0A71\u0A81-\u0A83\u0ABE-\u0AC5\u0AC7-\u0AC9" +
            "\u0ACB-\u0ACD\u0B01-\u0B03\u0B3E-\u0B43"
                + "\u0B47-\u0B48\u0B4B-\u0B4D\u0B56-\u0B57\u0B82-\u0B83\u0BBE-\u0BC2" +
            "\u0BC6-\u0BC8\u0BCA-\u0BCD\u0C01-\u0C03"
                + "\u0C3E-\u0C44\u0C46-\u0C48\u0C4A-\u0C4D\u0C55-\u0C56\u0C82-\u0C83" +
            "\u0CBE-\u0CC4\u0CC6-\u0CC8\u0CCA-\u0CCD"
                + "\u0CD5-\u0CD6\u0D02-\u0D03\u0D3E-\u0D43\u0D46-\u0D48\u0D4A-\u0D4D" +
            "\u0E34-\u0E3A\u0E47-\u0E4E\u0EB4-\u0EB9"
                + "\u0EBB-\u0EBC\u0EC8-\u0ECD\u0F18-\u0F19\u0F71-\u0F84\u0F86-\u0F8B" +
            "\u0F90-\u0F95\u0F99-\u0FAD\u0FB1-\u0FB7"
                + "\u20D0-\u20DC\u302A-\u302F\u05BF\u05C4\u0670\u093C\u094D\u09BC\u09BE" +
            "\u09BF\u09D7\u0A02\u0A3C\u0A3E\u0A3F"
                + "\u0ABC\u0B3C\u0BD7\u0D57\u0E31\u0EB1\u0F35\u0F37\u0F39\u0F3E" +
            "\u0F3F\u0F97\u0FB9\u20E1\u3099\u309A]", "Digit",
            "[\u0030-\u0039\u0660-\u0669\u06F0-\u06F9\u0966-\u096F\u09E6-\u09EF" +
                "\u0A66-\u0A6F\u0AE6-\u0AEF\u0B66-\u0B6F"
                + "\u0BE7-\u0BEF\u0C66-\u0C6F\u0CE6-\u0CEF\u0D66-\u0D6F\u0E50-\u0E59" +
                "\u0ED0-\u0ED9\u0F20-\u0F29]", "Ideographic", "[\u4E00-\u9FA5\u3021-\u3029\u3007]", "BaseChar",
            "[\u0041-\u005A\u0061-\u007A\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u00FF\u0100-\u0131\u0134-\u013E\u0141-\u0148"
                + "\u014A-\u017E\u0180-\u01C3\u01CD-\u01F0\u01F4-\u01F5\u01FA-\u0217\u0250-\u02A8\u02BB-\u02C1\u0388-\u038A"
                + "\u038E-\u03A1\u03A3-\u03CE\u03D0-\u03D6\u03E2-\u03F3\u0401-\u040C" +
                "\u040E-\u044F\u0451-\u045C\u045E-\u0481"
                + "\u0490-\u04C4\u04C7-\u04C8\u04CB-\u04CC\u04D0-\u04EB\u04EE-\u04F5" +
                "\u04F8-\u04F9\u0531-\u0556\u0561-\u0586"
                + "\u05D0-\u05EA\u05F0-\u05F2\u0621-\u063A\u0641-\u064A\u0671-\u06B7" +
                "\u06BA-\u06BE\u06C0-\u06CE\u06D0-\u06D3"
                + "\u06E5-\u06E6\u0905-\u0939\u0958-\u0961\u0985-\u098C\u098F-\u0990" +
                "\u0993-\u09A8\u09AA-\u09B0\u09B6-\u09B9"
                + "\u09DC-\u09DD\u09DF-\u09E1\u09F0-\u09F1\u0A05-\u0A0A\u0A0F-\u0A10" +
                "\u0A13-\u0A28\u0A2A-\u0A30\u0A32-\u0A33"
                + "\u0A35-\u0A36\u0A38-\u0A39\u0A59-\u0A5C\u0A72-\u0A74\u0A85-\u0A8B\u0A8F-\u0A91" +
                "\u0A93-\u0AA8\u0AAA-\u0AB0"
                + "\u0AB2-\u0AB3\u0AB5-\u0AB9\u0B05-\u0B0C\u0B0F-\u0B10\u0B13-\u0B28" +
                "\u0B2A-\u0B30\u0B32-\u0B33\u0B36-\u0B39"
                + "\u0B5C-\u0B5D\u0B5F-\u0B61\u0B85-\u0B8A\u0B8E-\u0B90" +
                "\u0B92-\u0B95\u0B99-\u0B9A\u0B9E-\u0B9F\u0BA3-\u0BA4"
                + "\u0BA8-\u0BAA\u0BAE-\u0BB5\u0BB7-\u0BB9\u0C05-\u0C0C" +
                "\u0C0E-\u0C10\u0C12-\u0C28\u0C2A-\u0C33\u0C35-\u0C39"
                + "\u0C60-\u0C61\u0C85-\u0C8C\u0C8E-\u0C90\u0C92-\u0CA8" +
                "\u0CAA-\u0CB3\u0CB5-\u0CB9\u0CE0-\u0CE1\u0D05-\u0D0C"
                + "\u0D0E-\u0D10\u0D12-\u0D28\u0D2A-\u0D39\u0D60-\u0D61" +
                "\u0E01-\u0E2E\u0E32-\u0E33\u0E40-\u0E45\u0E81-\u0E82"
                + "\u0E87-\u0E88\u0E94-\u0E97\u0E99-\u0E9F\u0EA1-\u0EA3" +
                "\u0EAA-\u0EAB\u0EAD-\u0EAE\u0EB2-\u0EB3\u0EC0-\u0EC4"
                + "\u0F40-\u0F47\u0F49-\u0F69\u10A0-\u10C5\u10D0-\u10F6" +
                "\u1102-\u1103\u1105-\u1107\u110B-\u110C\u110E-\u1112"
                + "\u1154-\u1155\u115F-\u1161\u116D-\u116E\u1172-\u1173" +
                "\u11AE-\u11AF\u11B7-\u11B8\u11BC-\u11C2\u1E00-\u1E9B"
                + "\u1EA0-\u1EF9\u1F00-\u1F15\u1F18-\u1F1D\u1F20-\u1F45" +
                "\u1F48-\u1F4D\u1F50-\u1F57\u1F5F-\u1F7D\u1F80-\u1FB4"
                + "\u1FB6-\u1FBC\u1FC2-\u1FC4\u1FC6-\u1FCC\u1FD0-\u1FD3" +
                "\u1FD6-\u1FDB\u1FE0-\u1FEC\u1FF2-\u1FF4\u1FF6-\u1FFC"
                + "\u212A-\u212B\u2180-\u2182\u3041-\u3094\u30A1-\u30FA" +
                "\u3105-\u312C\uAC00-\uD7A3"
                + "\u0386\u038C\u03DA\u03DC\u03DE\u03E0\u0559\u06D5\u093D" +
                "\u09B2\u0A5E\u0A8D\u0ABD\u0AE0\u0B3D\u0B9C\u0CDE"
                + "\u0E30\u0E84\u0E8A"
                + "\u0E8D\u0EA5\u0EA7\u0EB0\u0EBD\u1100\u1109\u113C\u113E" +
                "\u1140\u114C\u114E\u1150\u1159\u1163\u1165\u1167"
                + "\u1169\u1175\u119E"
                + "\u11A8\u11AB\u11BA\u11EB\u11F0\u11F9\u1F59\u1F5B" +
                "\u1F5D\u1FBE\u2126\u212E]", "Letter", "<BaseChar>|<Ideographic>", "NCNameChar",
            "<Letter>|<Digit>|[-._]|<CombiningChar>|<Extender>", "NameChar", "<NCNameChar>|:",
            "Nmtoken", "<NameChar>+", "NCName", "(<Letter>|_)<NCNameChar>*", "Name",
            "(<Letter>|[_:])<NameChar>*", "QName", "(<NCName>:)?<NCName>",
            "Char", "[\t\n\r\u0020-\uD7FF\ue000-\ufffd]|[\uD800-\uDBFF][\uDC00-\uDFFF]",
            "whitespacechar", "[ \t\n\r]"};

    Map<String, Automaton> t = buildMap(xmlexps);
    putFrom("NCName", t);
    putFrom("QName", t);
    putFrom("Char", t);
    putFrom("NameChar", t);
    putFrom("Letter", t);
    putFrom("whitespacechar", t);

    put(_automata, "whitespace", _ws);

    String[] uriexps =
        {"digit", "[0-9]", "upalpha", "[A-Z]", "lowalpha", "[a-z]", "alpha",
            "<lowalpha>|<upalpha>", "alphanum", "<alpha>|<digit>", "hex",
            "<digit>|[a-f]|[A-F]", "escaped", "%<hex><hex>", "mark",
            "[-_.!~*'()]", "unreserved", "<alphanum>|<mark>",
            // "reserved", "[;/?:@&=+$,]",
            "reserved", "[;/?:@&=+$,\\[\\]]",// RFC 2732
            "uric", "<reserved>|<unreserved>|<escaped>", "fragment",
            "<uric>*", "query", "<uric>*", "pchar", "<unreserved>|<escaped>|[:@&=+$,]",
            "param", "<pchar>*", "segment", "<pchar>*(;<param>)*", "path_segments",
            "<segment>(/<segment>)*", "abs_path", "/<path_segments>", "uric_no_slash",
            "<unreserved>|<escaped>|[;?:@&=+$,]", "opaque_part", "<uric_no_slash><uric>*",
            //"path", "(<abs_path>|<opaque_part>)?",  // not used
            "port", "<digit>*",
            // "IPv4address", "(<digit>{1,}\\.){3}<digit>{1,}",
            "IPv4address", "(<digit>{1,3}\\.){3}<digit>{1,3}", // RFC 2732 / 2373
            "hexseq", "<hex>{1,4}(:<hex>{1,4})*", // RFC 2373
            "hexpart", "<hexseq>|<hexseq>::<hexseq>?|::<hexseq>", // RFC 2373
            "IPv6address", "<hexpart>(:<IPv4address>)?", // RFC 2373
            "toplabel", "<alpha>|(<alpha>(<alphanum>|-)*<alphanum>)", "domainlabel",
            "<alphanum>|(<alphanum>(<alphanum>|-)*<alphanum>)", "hostname",
            "(<domainlabel>\\.)*<toplabel>\\.?",
            // "host", "<hostname>|<IPv4address>",
            "host", "<hostname>|<IPv4address>|\\[<IPv6address>\\]", // RFC 2732
            "hostport", "<host>(:<port>)?", "userinfo", "(<unreserved>|<escaped>|[;:&=+$,])*",
            "server", "((<userinfo>\\@)?<hostport>)?", "reg_name",
            "(<unreserved>|<escaped>|[$,;:@&=+])+", "authority", "<server>|<reg_name>",
            "scheme", "<alpha>(<alpha>|<digit>|[-+.])*", "rel_segment",
            "(<unreserved>|<escaped>|[;@&=+$,])+", "rel_path", "<rel_segment><abs_path>?",
            "net_path", "//<authority><abs_path>?", "hier_part",
            "(<net_path>|<abs_path>)(\\?<query>)?", "relativeURI",
            "(<net_path>|<abs_path>|<rel_path>)(\\?<query>)?", "absoluteURI",
            "<scheme>:(<hier_part>|<opaque_part>)", "URI",
            "(<absoluteURI>|<relativeURI>)?(\\#<fragment>)?"};
    putFrom("URI", buildMap(uriexps));
    put(_automata, "anyname", Automaton.minimize(Automaton.makeChar('{').
        concatenate(_automata.get("URI").clone()).
        concatenate(Automaton.makeChar('}')).
        optional().concatenate(_automata.get("NCName").clone())));

    put(_automata, "noap", new RegExp("~(@[@%]@)").toAutomaton());

    String[] xsdmisc =
        {"_", "[ \t\n\r]*", "d", "[0-9]", "Z", "[-+](<00-13>:<00-59>|14:00)|Z", "Y",
            "(<d>{4,})&~(0000)", "M", "<01-12>", "D", "<01-31>", "T",
            "<00-23>:<00-59>:<00-59>|24:00:00", "B64", "[A-Za-z0-9+/]", "B16",
            "[AEIMQUYcgkosw048]", "B04", "[AQgw]", "B04S", "<B04> ?", "B16S", "<B16> ?",
            "B64S", "<B64> ?",};
    String[] xsdexps =
        {"boolean", "<_>(true|false|1|0)<_>", "decimal", "<_>([-+]?<d>+(\\.<d>+)?)<_>", "float",
            "<_>([-+]?<d>+(\\.<d>+)?([Ee][-+]?<d>+)?|INF|-INF|NaN)<_>", "integer",
            "<_>[-+]?[0-9]+<_>", "duration",
            "<_>(-?P(((<d>+Y)?(<d>+M)?(<d>+D)?(T(((<d>+H)?(<d>+M)?(<d>+(\\.<d>+)?S)?)&~()))?)&~()))<_>",
            "dateTime", "<_>(-?<Y>-<M>-<D>T<T>(\\.<d>+)?<Z>?)<_>", "time", "<_>(<T>(\\.<d>+)?<Z>?)<_>",
            "date", "<_>(-?<Y>-<M>-<D><Z>?)<_>", "gYearMonth", "<_>(-?<Y>-<M><Z>?)<_>", "gYear",
            "<_>(-?<Y><Z>?)<_>", "gMonthDay", "<_>(--<M>-<D><Z>?)<_>", "gDay", "<_>(--<D><Z>?)<_>",
            "gMonth", "<_>(--<M><Z>?)<_>", "hexBinary", "<_>([0-9a-fA-F]{2}*)<_>", "base64Binary",
            "<_>(((<B64S><B64S><B64S><B64S>)*((<B64S><B64S><B64S><B64>)"
                + "|(<B64S><B64S><B16S>=)|(<B64S><B04S>= ?=)))?)<_>", "language",
            "<_>[a-zA-Z]{1,8}(-[a-zA-Z0-9]{1,8})*<_>", "nonPositiveInteger", "<_>(0+|-<d>+)<_>",
            "negativeInteger", "<_>(-[1-9]<d>*)<_>", "nonNegativeInteger", "<_>(<d>+)<_>",
            "positiveInteger", "<_>([1-9]<d>*)<_>",};
    Map<String, Automaton> m = buildMap(xsdmisc);
    putWith(xsdexps, m);

    put(m, "UNSIGNEDLONG", Automaton.makeMaxInteger("18446744073709551615"));
    put(m, "UNSIGNEDINT", Automaton.makeMaxInteger("4294967295"));
    put(m, "UNSIGNEDSHORT", Automaton.makeMaxInteger("65535"));
    put(m, "UNSIGNEDBYTE", Automaton.makeMaxInteger("255"));
    put(m, "LONG", Automaton.makeMaxInteger("9223372036854775807"));
    put(m, "LONG_NEG", Automaton.makeMaxInteger("9223372036854775808"));
    put(m, "INT", Automaton.makeMaxInteger("2147483647"));
    put(m, "INT_NEG", Automaton.makeMaxInteger("2147483648"));
    put(m, "SHORT", Automaton.makeMaxInteger("32767"));
    put(m, "SHORT_NEG", Automaton.makeMaxInteger("32768"));
    put(m, "BYTE", Automaton.makeMaxInteger("127"));
    put(m, "BYTE_NEG", Automaton.makeMaxInteger("128"));

    Map<String, Automaton> u = new HashMap<String, Automaton>();
    u.putAll(t);
    u.putAll(m);
    String[] xsdexps2 =
        {"Nmtoken2", "<_><Nmtoken><_>", "Name2", "<_><Name><_>", "NCName2",
            "<_><NCName><_>", "QName2", "<_><QName><_>", "Nmtokens", "<_>(<Nmtoken><_>)+",
            "NCNames", "<_>(<NCName><_>)+", "Names", "<_>(<Name><_>)+", "unsignedLong",
            "<_><UNSIGNEDLONG><_>", "unsignedInt", "<_><UNSIGNEDINT><_>", "unsignedShort",
            "<_><UNSIGNEDSHORT><_>", "unsignedByte", "<_><UNSIGNEDBYTE><_>", "long",
            "<_>(<LONG>|-<LONG_NEG>)<_>", "int", "<_>(<INT>|-<INT_NEG>)<_>", "short",
            "<_>(<SHORT>|-<SHORT_NEG>)<_>", "byte", "<_>(<BYTE>|-<BYTE_NEG>)<_>", "string",
            "<Char>*"};
    putWith(xsdexps2, u);

    put(_automata, "BasicLatin", Automaton.makeCharRange('\u0000', '\u007F'));
    put(_automata, "Latin-1Supplement", Automaton.makeCharRange('\u0080', '\u00FF'));
    put(_automata, "LatinExtended-A", Automaton.makeCharRange('\u0100', '\u017F'));
    put(_automata, "LatinExtended-B", Automaton.makeCharRange('\u0180', '\u024F'));
    put(_automata, "IPAExtensions", Automaton.makeCharRange('\u0250', '\u02AF'));
    put(_automata, "SpacingModifierLetters", Automaton.makeCharRange('\u02B0', '\u02FF'));
    put(_automata, "CombiningDiacriticalMarks", Automaton.makeCharRange('\u0300', '\u036F'));
    put(_automata, "Greek", Automaton.makeCharRange('\u0370', '\u03FF'));
    put(_automata, "Cyrillic", Automaton.makeCharRange('\u0400', '\u04FF'));
    put(_automata, "Armenian", Automaton.makeCharRange('\u0530', '\u058F'));
    put(_automata, "Hebrew", Automaton.makeCharRange('\u0590', '\u05FF'));
    put(_automata, "Arabic", Automaton.makeCharRange('\u0600', '\u06FF'));
    put(_automata, "Syriac", Automaton.makeCharRange('\u0700', '\u074F'));
    put(_automata, "Thaana", Automaton.makeCharRange('\u0780', '\u07BF'));
    put(_automata, "Devanagari", Automaton.makeCharRange('\u0900', '\u097F'));
    put(_automata, "Bengali", Automaton.makeCharRange('\u0980', '\u09FF'));
    put(_automata, "Gurmukhi", Automaton.makeCharRange('\u0A00', '\u0A7F'));
    put(_automata, "Gujarati", Automaton.makeCharRange('\u0A80', '\u0AFF'));
    put(_automata, "Oriya", Automaton.makeCharRange('\u0B00', '\u0B7F'));
    put(_automata, "Tamil", Automaton.makeCharRange('\u0B80', '\u0BFF'));
    put(_automata, "Telugu", Automaton.makeCharRange('\u0C00', '\u0C7F'));
    put(_automata, "Kannada", Automaton.makeCharRange('\u0C80', '\u0CFF'));
    put(_automata, "Malayalam", Automaton.makeCharRange('\u0D00', '\u0D7F'));
    put(_automata, "Sinhala", Automaton.makeCharRange('\u0D80', '\u0DFF'));
    put(_automata, "Thai", Automaton.makeCharRange('\u0E00', '\u0E7F'));
    put(_automata, "Lao", Automaton.makeCharRange('\u0E80', '\u0EFF'));
    put(_automata, "Tibetan", Automaton.makeCharRange('\u0F00', '\u0FFF'));
    put(_automata, "Myanmar", Automaton.makeCharRange('\u1000', '\u109F'));
    put(_automata, "Georgian", Automaton.makeCharRange('\u10A0', '\u10FF'));
    put(_automata, "HangulJamo", Automaton.makeCharRange('\u1100', '\u11FF'));
    put(_automata, "Ethiopic", Automaton.makeCharRange('\u1200', '\u137F'));
    put(_automata, "Cherokee", Automaton.makeCharRange('\u13A0', '\u13FF'));
    put(_automata, "UnifiedCanadianAboriginalSyllabics", Automaton.makeCharRange('\u1400', '\u167F'));
    put(_automata, "Ogham", Automaton.makeCharRange('\u1680', '\u169F'));
    put(_automata, "Runic", Automaton.makeCharRange('\u16A0', '\u16FF'));
    put(_automata, "Khmer", Automaton.makeCharRange('\u1780', '\u17FF'));
    put(_automata, "Mongolian", Automaton.makeCharRange('\u1800', '\u18AF'));
    put(_automata, "LatinExtendedAdditional", Automaton.makeCharRange('\u1E00', '\u1EFF'));
    put(_automata, "GreekExtended", Automaton.makeCharRange('\u1F00', '\u1FFF'));
    put(_automata, "GeneralPunctuation", Automaton.makeCharRange('\u2000', '\u206F'));
    put(_automata, "SuperscriptsandSubscripts", Automaton.makeCharRange('\u2070', '\u209F'));
    put(_automata, "CurrencySymbols", Automaton.makeCharRange('\u20A0', '\u20CF'));
    put(_automata, "CombiningMarksforSymbols", Automaton.makeCharRange('\u20D0', '\u20FF'));
    put(_automata, "LetterlikeSymbols", Automaton.makeCharRange('\u2100', '\u214F'));
    put(_automata, "NumberForms", Automaton.makeCharRange('\u2150', '\u218F'));
    put(_automata, "Arrows", Automaton.makeCharRange('\u2190', '\u21FF'));
    put(_automata, "MathematicalOperators", Automaton.makeCharRange('\u2200', '\u22FF'));
    put(_automata, "MiscellaneousTechnical", Automaton.makeCharRange('\u2300', '\u23FF'));
    put(_automata, "ControlPictures", Automaton.makeCharRange('\u2400', '\u243F'));
    put(_automata, "OpticalCharacterRecognition", Automaton.makeCharRange('\u2440', '\u245F'));
    put(_automata, "EnclosedAlphanumerics", Automaton.makeCharRange('\u2460', '\u24FF'));
    put(_automata, "BoxDrawing", Automaton.makeCharRange('\u2500', '\u257F'));
    put(_automata, "BlockElements", Automaton.makeCharRange('\u2580', '\u259F'));
    put(_automata, "GeometricShapes", Automaton.makeCharRange('\u25A0', '\u25FF'));
    put(_automata, "MiscellaneousSymbols", Automaton.makeCharRange('\u2600', '\u26FF'));
    put(_automata, "Dingbats", Automaton.makeCharRange('\u2700', '\u27BF'));
    put(_automata, "BraillePatterns", Automaton.makeCharRange('\u2800', '\u28FF'));
    put(_automata, "CJKRadicalsSupplement", Automaton.makeCharRange('\u2E80', '\u2EFF'));
    put(_automata, "KangxiRadicals", Automaton.makeCharRange('\u2F00', '\u2FDF'));
    put(_automata, "IdeographicDescriptionCharacters",
        Automaton.makeCharRange('\u2FF0', '\u2FFF'));
    put(_automata, "CJKSymbolsandPunctuation", Automaton.makeCharRange('\u3000', '\u303F'));
    put(_automata, "Hiragana", Automaton.makeCharRange('\u3040', '\u309F'));
    put(_automata, "Katakana", Automaton.makeCharRange('\u30A0', '\u30FF'));
    put(_automata, "Bopomofo", Automaton.makeCharRange('\u3100', '\u312F'));
    put(_automata, "HangulCompatibilityJamo", Automaton.makeCharRange('\u3130', '\u318F'));
    put(_automata, "Kanbun", Automaton.makeCharRange('\u3190', '\u319F'));
    put(_automata, "BopomofoExtended", Automaton.makeCharRange('\u31A0', '\u31BF'));
    put(_automata, "EnclosedCJKLettersandMonths", Automaton.makeCharRange('\u3200', '\u32FF'));
    put(_automata, "CJKCompatibility", Automaton.makeCharRange('\u3300', '\u33FF'));
    put(_automata, "CJKUnifiedIdeographsExtensionA", Automaton.makeCharRange('\u3400', '\u4DB5'));
    put(_automata, "CJKUnifiedIdeographs", Automaton.makeCharRange('\u4E00', '\u9FFF'));
    put(_automata, "YiSyllables", Automaton.makeCharRange('\uA000', '\uA48F'));
    put(_automata, "YiRadicals", Automaton.makeCharRange('\uA490', '\uA4CF'));
    put(_automata, "HangulSyllables", Automaton.makeCharRange('\uAC00', '\uD7A3'));
    put(_automata, "CJKCompatibilityIdeographs", Automaton.makeCharRange('\uF900', '\uFAFF'));
    put(_automata, "AlphabeticPresentationForms", Automaton.makeCharRange('\uFB00', '\uFB4F'));
    put(_automata, "ArabicPresentationForms-A", Automaton.makeCharRange('\uFB50', '\uFDFF'));
    put(_automata, "CombiningHalfMarks", Automaton.makeCharRange('\uFE20', '\uFE2F'));
    put(_automata, "CJKCompatibilityForms", Automaton.makeCharRange('\uFE30', '\uFE4F'));
    put(_automata, "SmallFormVariants", Automaton.makeCharRange('\uFE50', '\uFE6F'));
    put(_automata, "ArabicPresentationForms-B", Automaton.makeCharRange('\uFE70', '\uFEFE'));
    put(_automata, "Specials", Automaton.makeCharRange('\uFEFF', '\uFEFF'));
    put(_automata, "HalfwidthandFullwidthForms", Automaton.makeCharRange('\uFF00', '\uFFEF'));
    put(_automata, "Specials", Automaton.makeCharRange('\uFFF0', '\uFFFD'));

    put(_automata, "OldItalic", Automaton.makeChar('\ud800')
        .concatenate(Automaton.makeCharRange('\udf00', '\udf2f')));
    put(_automata, "Gothic", Automaton.makeChar('\ud800')
        .concatenate(Automaton.makeCharRange('\udf30', '\udf4f')));
    put(_automata, "Deseret", Automaton.makeChar('\ud801')
        .concatenate(Automaton.makeCharRange('\udc00', '\udc4f')));
    put(_automata, "ByzantineMusicalSymbols",
        Automaton.makeChar('\ud834').concatenate(Automaton.makeCharRange('\udc00', '\udcff')));
    put(_automata, "MusicalSymbols",
        Automaton.makeChar('\ud834').concatenate(Automaton.makeCharRange('\udd00', '\uddff')));
    put(_automata, "MathematicalAlphanumericSymbols",
        Automaton.makeChar('\ud835').concatenate(Automaton.makeCharRange('\udc00', '\udfff')));

    put(_automata, "CJKUnifiedIdeographsExtensionB",
        Automaton.makeCharRange('\ud840', '\ud868')
            .concatenate(Automaton.makeCharRange('\udc00', '\udfff'))
            .union(Automaton.makeChar('\ud869')
                .concatenate(Automaton.makeCharRange('\udc00', '\uded6'))));

    put(_automata, "CJKCompatibilityIdeographsSupplement",
        Automaton.makeChar('\ud87e').concatenate(Automaton.makeCharRange('\udc00', '\ude1f')));
    put(_automata, "Tags", Automaton.makeChar('\udb40')
        .concatenate(Automaton.makeCharRange('\udc00', '\udc7f')));

    put(_automata, "PrivateUse", Automaton.makeCharRange('\uE000', '\uF8FF').union(
        Automaton.makeCharRange('\udb80', '\udbbe')
            .concatenate(Automaton.makeCharRange('\udc00', '\udfff'))
            .union(Automaton.makeChar('\udbbf')
                .concatenate(Automaton.makeCharRange('\udc00', '\udffd')))).union(
        Automaton.makeCharRange('\udbc0', '\udbfe')
            .concatenate(Automaton.makeCharRange('\udc00', '\udfff'))
            .union(Automaton.makeChar('\udbff')
                .concatenate(Automaton.makeCharRange('\udc00', '\udffd')))));

    Map<String, Set<Integer>> categories = new HashMap<String, Set<Integer>>();
    try {
      StreamTokenizer st = new StreamTokenizer(new BufferedReader(
          new FileReader("src/Unicode.txt")));
      st.resetSyntax();
      st.whitespaceChars(';', ';');
      st.whitespaceChars('\n', ' ');
      st.wordChars('0', '9');
      st.wordChars('a', 'z');
      st.wordChars('A', 'Z');
      while (st.nextToken() != StreamTokenizer.TT_EOF) {
        int cp = Integer.parseInt(st.sval, 16);
        st.nextToken();
        String cat = st.sval;
        Set<Integer> c = categories.get(cat);
        if (c == null) {
          c = new TreeSet<>();
          categories.put(cat, c);
        }
        c.add(cp);
        String ccat = cat.substring(0, 1);
        c = categories.get(ccat);
        if (c == null) {
          c = new TreeSet<Integer>();
          categories.put(ccat, c);
        }
        c.add(cp);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    List<Automaton> assigned = new ArrayList<Automaton>();
    for (Map.Entry<String, Set<Integer>> me : categories.entrySet()) {
      List<Automaton> la1 = new ArrayList<Automaton>();
      List<Automaton> la2 = new ArrayList<Automaton>();
      for (Integer cp : me.getValue()) {
        la1.add(makeCodePoint(cp));
        if (la1.size() == 50) {
          la2.add(Automaton.minimize(Automaton.union(la1)));
          la1.clear();
        }
      }
      la2.add(Automaton.union(la1));
      Automaton a = Automaton.minimize(Automaton.union(la2));
      put(_automata, me.getKey(), a);
      assigned.add(a);
    }
    Automaton cn =
        Automaton.minimize(_automata.get("Char").clone()
            .intersection(Automaton.union(assigned).complement()));
    put(_automata, "Cn", cn);
    put(_automata, "C", _automata.get("C").clone().union(cn));
  }

  private static Automaton makeCodePoint(int cp) {
    if (cp >= 0x10000) {
      cp -= 0x10000;
      char[] cu = {(char) (0xd800 + (cp >> 10)), (char) (0xdc00 + (cp & 0x3ff))};
      return Automaton.makeString(new String(cu));
    } else {
      return Automaton.makeChar((char) cp);
    }
  }

  private static Map<String, Automaton> buildMap(String[] exps) {
    Map<String, Automaton> map = new HashMap<String, Automaton>();
    int i = 0;
    while (i + 1 < exps.length) {
      put(map, exps[i++], new RegExp(exps[i++]).toAutomaton(map));
    }
    return map;
  }

  private static void putWith(String[] exps, Map<String, Automaton> use) {
    int i = 0;
    while (i + 1 < exps.length) {
      put(_automata, exps[i++], new RegExp(exps[i++]).toAutomaton(use));
    }
  }

  private static void putFrom(String name, Map<String, Automaton> from) {
    _automata.put(name, from.get(name));
  }

  private static void put(Map<String, Automaton> map, String name, Automaton a) {
    map.put(name, a);
  }

  static {
    _automata = new HashMap<>();
    _ws = Automaton.minimize(Automaton.makeCharSet(" \t\n\r").repeat());
    _unicodeBlockNames = new HashSet<>(Arrays.asList(_unicodeBlockNamesArray));
    _unicodeCategoryNames = new HashSet<>(Arrays.asList(_unicodeCategoryNamesArray));
    _xmlNames = new HashSet<>(Arrays.asList(_xmlNamesArray));
  }
}
