//
// Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

/*******************************************************************************************

BNF Grammar for PQL
===================

<statement> ::= ( <select_stmt> | <describe_stmt> ) [';']

<select_stmt> ::= SELECT <select_list> [<from_clause>] [<where_clause>] [<given_clause>]
                  [<additional_clauses>]

<describe_stmt> ::= ( DESC | DESCRIBE ) [<index_name>]

<select_list> ::= '*' | (<column_name>|<aggregation_function>)( ',' <column_name>|<aggregation_function> )*

<column_name_list> ::= <column_name> ( ',' <column_name> )*

<aggregation_function> ::= <function_name> '(' <column_name> ')'

<function_name> ::= <column_name>

<from_clause> ::= FROM <index_name>

<index_name> ::= <identifier> | <quoted_string>

<where_clause> ::= WHERE <search_expr>

<search_expr> ::= <term_expr> ( OR <term_expr> )*

<term_expr> ::= <facet_expr> ( AND <facet_expr> )*

<facet_expr> ::= <predicate> 
               | '(' <search_expr> ')'

<predicates> ::= <predicate> ( AND <predicate> )*

<predicate> ::= <in_predicate>
              | <contains_all_predicate>
              | <equal_predicate>
              | <not_equal_predicate>
              | <query_predicate>
              | <between_predicate>
              | <range_predicate>
              | <time_predicate>
              | <match_predicate>
              | <like_predicate>
              | <null_predicate>

<in_predicate> ::= <column_name> [NOT] IN <value_list> [<except_clause>] [<predicate_props>]

<contains_all_predicate> ::= <column_name> CONTAINS ALL <value_list> [<except_clause>]
                             [<predicate_props>]

<equal_predicate> ::= <column_name> '=' <value> [<predicate_props>]

<not_equal_predicate> ::= <column_name> '<>' <value> [<predicate_props>]

<query_predicate> ::= QUERY IS <quoted_string>

<between_predicate> ::= <column_name> [NOT] BETWEEN <value> AND <value>

<range_predicate> ::= <column_name> <range_op> <numeric>

<time_predicate> ::= <column_name> IN LAST <time_span>
                   | <column_name> ( SINCE | AFTER | BEFORE ) <time_expr>

<match_predicate> ::= [NOT] MATCH '(' <column_name_list> ')' AGAINST '(' <quoted_string> ')'

<like_predicate> ::= <column_name> [NOT] LIKE <quoted_string>

<null_predicate> ::= <column_name> IS [NOT] NULL

<value_list> ::= <non_variable_value_list> | <variable>

<non_variable_value_list> ::= '(' <value> ( ',' <value> )* ')'

<python_style_list> ::= '[' <python_style_value>? ( ',' <python_style_value> )* ']'

<python_style_dict> ::= '{''}' 
                       | '{' <key_value_pair> ( ',' <key_value_pair> )* '}'

<python_style_value> ::= <value>
                       | <python_style_list>
                       | <python_style_dict>

<value> ::= <numeric>
          | <quoted_string>
          | TRUE
          | FALSE
          | <variable>

<range_op> ::= '<' | '<=' | '>=' | '>'

<except_clause> ::= EXCEPT <value_list>

<predicate_props> ::= WITH <prop_list>

<prop_list> ::= '(' <key_value_pair> ( ',' <key_value_pair> )* ')'

<key_value_pair> ::= <quoted_string> ':' 
                     ( <value> | <python_style_list> | <python_style_dict> )

<given_clause> ::= GIVEN FACET PARAM <facet_param_list>

<facet_param_list> ::= <facet_param> ( ',' <facet_param> )*

<facet_param> ::= '(' <facet_name> <facet_param_name> <facet_param_type> <facet_param_value> ')'

<facet_param_name> ::= <quoted_string>

<facet_param_type> ::= BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE

<facet_param_value> ::= <quoted_string>

<additional_clauses> ::= ( <additional_clause> )+

<additional_clause> ::= <order_by_clause>
                      | <limit_clause>
                      | <group_by_clause>
                      | <distinct_clause>
                      | <execute_clause>
                      | <browse_by_clause>
                      | <fetching_stored_clause>
                      | <route_by_clause>
                      | <relevance_model_clause>

<order_by_clause> ::= ORDER BY <sort_specs>

<sort_specs> ::= <sort_spec> ( ',' <sort_spec> )*

<sort_spec> ::= <column_name> [<ordering_spec>]

<ordering_spec> ::= ASC | DESC

<group_by_clause> ::= GROUP BY <group_spec>

<distinct_clause> ::= DISTINCT <distinct_spec>

<execute_clause> ::= EXECUTE '(' function_name ((',' python_style_dict) | (',' key_value_pair)*) ')'

<group_spec> ::= <comma_column_name_list> [TOP <max_per_group>]

<distinct_spec> ::= <or_column_name_list>

<or_column_name_list> ::= <column_name> ( OR <column_name> )*

<comma_column_name_list> ::= <column_name> ( (OR | ',') <column_name> )*

<limit_clause> ::= LIMIT [<offset> ','] <count>

<offset> ::= ( <digit> )+

<count> ::= ( <digit> )+

<browse_by_clause> ::= BROWSE BY <facet_specs>

<facet_specs> ::= <facet_spec> ( ',' <facet_spec> )*

<facet_spec> ::= <facet_name> [<facet_expression>]

<facet_expression> ::= '(' <expand_flag> <count> <count> <facet_ordering> ')'

<expand_flag> ::= TRUE | FALSE

<facet_ordering> ::= HITS | VALUE

<fetching_stored_clause> ::= FETCHING STORED [<fetching_flag>]

<fetching_flag> ::= TRUE | FALSE

<route_by_clause> ::= ROUTE BY <quoted_string>

<relevance_model_clause> ::= USING RELEVANCE MODEL <identifier> <prop_list>
                             [<relevance_model>]

<relevance_model> ::= DEFINED AS <formal_parameters> BEGIN <model_block> END

<formal_parameters> ::= '(' <formal_parameter_decls> ')'

<formal_parameter_decls> ::= <formal_parameter_decl> ( ',' <formal_parameter_decl> )*

<formal_parameter_decl> ::= <variable_modifiers> <type> <variable_declarator_id>

<variable_modifiers> ::= ( <variable_modifier> )*

<variable_modifier> ::= 'final'

<type> ::= <class_or_interface_type> ('[' ']')*
         | <primitive_type> ('[' ']')*
         | <boxed_type> ('[' ']')*
         | <limited_type> ('[' ']')*

<class_or_interface_type> ::= <fast_util_data_type>

<fast_util_data_type> ::= 'IntOpenHashSet'
                        | 'FloatOpenHashSet'
                        | 'DoubleOpenHashSet'
                        | 'LongOpenHashSet'
                        | 'ObjectOpenHashSet'
                        | 'Int2IntOpenHashMap'
                        | 'Int2FloatOpenHashMap'
                        | 'Int2DoubleOpenHashMap'
                        | 'Int2LongOpenHashMap'
                        | 'Int2ObjectOpenHashMap'
                        | 'Object2IntOpenHashMap'
                        | 'Object2FloatOpenHashMap'
                        | 'Object2DoubleOpenHashMap'
                        | 'Object2LongOpenHashMap'
                        | 'Object2ObjectOpenHashMap'

<primitive_type> ::= 'boolean' | 'char' | 'byte' | 'short' 
                   | 'int' | 'long' | 'float' | 'double'

<boxed_type> ::= 'Boolean' | 'Character' | 'Byte' | 'Short' 
               | 'Integer' | 'Long' | 'Float' | 'Double'

<limited_type> ::= 'String' | 'System' | 'Math'

<model_block> ::= ( <block_statement> )+

<block_statement> ::= <local_variable_declaration_stmt>
                    | <java_statement>

<local_variable_declaration_stmt> ::= <local_variable_declaration> ';'

<local_variable_declaration> ::= <variable_modifiers> <type> <variable_declarators>

<java_statement> ::= <block>
                   | 'if' <par_expression> <java_statement> [ <else_statement> ]
                   | 'for' '(' <for_control> ')' <java_statement>
                   | 'while' <par_expression> <java_statement>
                   | 'do' <java_statement> 'while> <par_expression> ';'
                   | 'switch' <par_expression> '{' <switch_block_statement_groups> '}'
                   | 'return' <expression> ';'
                   | 'break' [<identifier>] ';'
                   | 'continue' [<identifier>] ';'
                   | ';'
                   | <statement_expression> ';'

<block> ::= '{' ( <block_statement> )* '}'

<else_statement> ::= 'else' <java_statement>

<switch_block_statement_groups> ::= ( <switch_block_statement_group> )*

<switch_block_statement_group> ::= ( <switch_label> )+ ( <block_statement> )*

<switch_label> ::= 'case' <constant_expression> ':'
                 | 'case' <enum_constant_name> ':'
                 | 'default' ':'

<for_control> ::= <enhanced_for_control>
                | [<for_init>] ';' [<expression>] ';' [<for_update>]

<for_init> ::= <local_variable_declaration>
             | <expression_list>

<enhanced_for_control> ::= <variable_modifiers> <type> <identifier> ':' <expression>

<for_update> ::= <expression_list>

<par_expression> ::= '(' <expression> ')'

<expression_list> ::= <expression> ( ',' <expression> )*

<statement_expression> ::= <expression>

<constant_expression> ::= <expression>

<enum_constant_name> ::= <identifier>

<variable_declarators> ::= <variable_declarator> ( ',' <variable_declarator> )*

<variable_declarator> ::= <variable_declarator_id> '=' <variable_initializer>

<variable_declarator_id> ::= <identifier> ('[' ']')*

<variable_initializer> ::= <array_initializer>
                         | <expression>

<array_initializer> ::= '{' [ <variable_initializer> ( ',' <variable_initializer> )* [','] ] '}'

<expression> ::= <conditional_expression> [ <assignment_operator> <expression> ]

<assignment_operator> ::= '=' | '+=' | '-=' | '*=' | '/=' | '&=' | '|=' | '^=' |
                        | '%=' | '<<=' | '>>>=' | '>>='

<conditional_expression> ::= <conditional_or_expression> [ '?' <expression> ':' <expression> ]

<conditional_or_expression> ::= <conditional_and_expression> ( '||' <conditional_and_expression> )*

<conditional_and_expression> ::= <inclusive_or_expression> ('&&' <inclusive_or_expression> )*

<inclusive_or_expression> ::= <exclusive_or_expression> ('|' <exclusive_or_expression> )*

<exclusive_or_expression> ::= <and_expression> ('^' <and_expression> )*

<and_expression> ::= <equality_expression> ( '&' <equality_expression> )*

<equality_expression> ::= <instanceof_expression> ( ('==' | '!=') <instanceof_expression> )*

<instanceof_expression> ::= <relational_expression> [ 'instanceof' <type> ]

<relational_expression> ::= <shift_expression> ( <relational_op> <shift_expression> )*

<shift_expression> ::= <additive_expression> ( <shift_op> <additive_expression> )*

<relational_op> ::= '<=' | '>=' | '<' | '>'

<shift_op> ::= '<<' | '>>>' | '>>'

<additive_expression> ::= <multiplicative_expression> ( ('+' | '-') <multiplicative_expression> )*

<multiplicative_expression> ::= <unary_expression> ( ( '*' | '/' | '%' ) <unary_expression> )*

<unary_expression> ::= '+' <unary_expression>
                     | '-' <unary_expression>
                     | '++' <unary_expression>
                     | '--' <unary_expression>
                     | <unary_expression_not_plus_minus>

<unary_expression_not_plus_minus> ::= '~' <unary_expression>
                                    | '!' <unary_expression>
                                    | <cast_expression>
                                    | <primary> <selector>* [ ('++'|'--') ]

<cast_expression> ::= '(' <primitive_type> ')' <unary_expression>
                    | '(' (<type> | <expression>) ')' <unary_expression_not_plus_minus>

<primary> ::= <par_expression>
            | <literal>
            | java_method identifier_suffix
            | <java_ident> ('.' <java_method>)* [<identifier_suffix>]

<java_ident> ::= <boxed_type>
               | <limited_type>
               | <identifier>

<java_method> ::= <identifier>

<identifier_suffix> ::= ('[' ']')+ '.' 'class'
                      | <arguments>
                      | '.' 'class'
                      | '.' 'this'
                      | '.' 'super' <arguments>

<literal> ::= <integer>
            | <real>
            | <floating_point_literal>
            | <character_literal>
            | <quoted_string>
            | <boolean_literal>
            | 'null'

<boolean_literal> ::= 'true' | 'false'

<selector> ::= '.' <identifier> <arguments>
             | '.' 'this'
             | '[' <expression> ']'

<arguments> ::= '(' [<expression_list>] ')'

<quoted_string> ::= '"' ( <char> )* '"'
                  | "'" ( <char> )* "'"

<identifier> ::= <identifier_start> ( <identifier_part> )*

<identifier_start> ::= <alpha> | '-' | '_'

<identifier_part> ::= <identifier_start> | <digit>

<variable> ::= '$' ( <alpha> | <digit> | '_' )+

<column_name> ::= <identifier> | <quoted_string>

<facet_name> ::= <identifier>

<alpha> ::= <alpha_lower_case> | <alpha_upper_case>

<alpha_upper_case> ::= A | B | C | D | E | F | G | H | I | J | K | L | M | N | O
                     | P | Q | R | S | T | U | V | W | X | Y | Z

<alpha_lower_case> ::= a | b | c | d | e | f | g | h | i | j | k | l | m | n | o
                     | p | q | r | s | t | u | v | w | x | y | z

<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9

<numeric> ::= <time_expr> 
            | <integer>
            | <real>

<integer> ::= ( <digit> )+

<real> ::= ( <digit> )+ '.' ( <digit> )+

<time_expr> ::= <time_span> AGO
              | <date_time_string>
              | NOW

<time_span> ::= [<time_week_part>] [<time_day_part>] [<time_hour_part>]
                [<time_minute_part>] [<time_second_part>] [<time_millisecond_part>]

<time_week_part> ::= <integer> ( 'week' | 'weeks' )

<time_day_part>  ::= <integer> ( 'day'  | 'days' )

<time_hour_part> ::= <integer> ( 'hour' | 'hours' )

<time_minute_part> ::= <integer> ( 'minute' | 'minutes' | 'min' | 'mins')

<time_second_part> ::= <integer> ( 'second' | 'seconds' | 'sec' | 'secs')

<time_millisecond_part> ::= <integer> ( 'millisecond' | 'milliseconds' | 'msec' | 'msecs')

<date_time_string> ::= <date> [<time>]

<date> ::= <digit><digit><digit><digit> ('-' | '/' | '.') <digit><digit>
           ('-' | '/' | '.') <digit><digit>

<time> ::= DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT

*******************************************************************************************/

grammar PQL;

options
{
  // ANTLR will generate java lexer and parser
  language = Java;
  // Generated parser should create abstract syntax tree
  output = AST;

  backtrack = true;
  memoize = true;
}

// Imaginary tokens
tokens 
{
    COLUMN_LIST;
    OR_PRED;
    AND_PRED;
    EQUAL_PRED;
    RANGE_PRED;
}

// As the generated lexer will reside in com.linkedin.pinot.pql.parsers package,
// we have to add package declaration on top of it
@lexer::header {
package com.linkedin.pinot.pql.parsers;
}

@lexer::members {

  // @Override
  // public void reportError(RecognitionException e) {
  //   throw new IllegalArgumentException(e);
  // }

}

// As the generated parser will reside in com.linkedin.pinot.pql.parsers
// package, we have to add package declaration on top of it
@parser::header {
package com.linkedin.pinot.pql.parsers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.linkedin.pinot.pql.parsers.utils.JSONUtil.FastJSONArray;
import com.linkedin.pinot.pql.parsers.utils.JSONUtil.FastJSONObject;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.pinot.pql.parsers.utils.PQLParserUtils;
}

@parser::members {

    private static enum KeyType {
      STRING_LITERAL,
      IDENT,
      STRING_LITERAL_AND_IDENT
    }

    private static final int DEFAULT_REQUEST_OFFSET = 0;
    private static final int DEFAULT_REQUEST_COUNT = 10;
    private static final int DEFAULT_REQUEST_MAX_PER_GROUP = 10;
    private static final int DEFAULT_FACET_MINHIT = 1;
    private static final int DEFAULT_FACET_MAXHIT = 10;
    private static final Map<String, String> _fastutilTypeMap;
    private static final Map<String, String> _internalVarMap;
    private static final Map<String, String> _internalStaticVarMap;    
    private static final Set<String> _supportedClasses;
    private static Map<String, Set<String>> _compatibleFacetTypes;

    private Map<String, String[]> _facetInfoMap;
    private long _now;
    private HashSet<String> _variables;
    private SimpleDateFormat[] _format1 = new SimpleDateFormat[2];
    private SimpleDateFormat[] _format2 = new SimpleDateFormat[2];

    private LinkedList<Map<String, String>> _symbolTable;
    private Map<String, String> _currentScope;
    private Set<String> _usedFacets; // Facets used by relevance model
    private Set<String> _usedInternalVars; // Internal variables used by relevance model

    static {
        _fastutilTypeMap = new HashMap<String, String>();
        _fastutilTypeMap.put("IntOpenHashSet", "set_int");
        _fastutilTypeMap.put("FloatOpenHashSet", "set_float");
        _fastutilTypeMap.put("DoubleOpenHashSet", "set_double");
        _fastutilTypeMap.put("LongOpenHashSet", "set_long");
        _fastutilTypeMap.put("ObjectOpenHashSet", "set_string");

        _fastutilTypeMap.put("Int2IntOpenHashMap", "map_int_int");
        _fastutilTypeMap.put("Int2FloatOpenHashMap", "map_int_float");
        _fastutilTypeMap.put("Int2DoubleOpenHashMap", "map_int_double");
        _fastutilTypeMap.put("Int2LongOpenHashMap", "map_int_long");
        _fastutilTypeMap.put("Int2ObjectOpenHashMap", "map_int_string");

        _fastutilTypeMap.put("Object2IntOpenHashMap", "map_string_int");
        _fastutilTypeMap.put("Object2FloatOpenHashMap", "map_string_float");
        _fastutilTypeMap.put("Object2DoubleOpenHashMap", "map_string_double");
        _fastutilTypeMap.put("Object2LongOpenHashMap", "map_string_long");
        _fastutilTypeMap.put("Object2ObjectOpenHashMap", "map_string_string");

        _internalVarMap = new HashMap<String, String>();
        _internalVarMap.put("_NOW", "long");
        _internalVarMap.put("_INNER_SCORE", "float");
        _internalVarMap.put("_RANDOM", "java.util.Random");
        
        _internalStaticVarMap = new HashMap<String, String>();
        _internalStaticVarMap.put("_RANDOM", "java.util.Random");

        _supportedClasses = new HashSet<String>();
        _supportedClasses.add("Boolean");
        _supportedClasses.add("Byte");
        _supportedClasses.add("Character");
        _supportedClasses.add("Double");
        _supportedClasses.add("Integer");
        _supportedClasses.add("Long");
        _supportedClasses.add("Short");

        _supportedClasses.add("Math");
        _supportedClasses.add("String");
        _supportedClasses.add("System");
        
        _compatibleFacetTypes = new HashMap<String, Set<String>>();
        _compatibleFacetTypes.put("range", new HashSet<String>(Arrays.asList(new String[]
                                                               {
                                                                   "simple",
                                                                   "multi"
                                                               })));
    }

    public PQLParser(TokenStream input, Map<String, String[]> facetInfoMap)
    {
        this(input);
        _facetInfoMap = facetInfoMap;
        _facetInfoMap.put("_uid", new String[]{"simple", "long"});
    }

    // The following two overridden methods are used to force ANTLR to
    // stop parsing upon the very first error.

    // @Override
    // protected void mismatch(IntStream input, int ttype, BitSet follow)
    //     throws RecognitionException
    // {
    //     throw new MismatchedTokenException(ttype, input);
    // }

    @Override
    protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
        throws RecognitionException
    {
        throw new MismatchedTokenException(ttype, input);
    }

    @Override
    public Object recoverFromMismatchedSet(IntStream input,
                                           RecognitionException e,
                                           BitSet follow)
        throws RecognitionException
    {
        throw e;
    }

    @Override
    public String getErrorMessage(RecognitionException err, String[] tokenNames) 
    {
        List stack = getRuleInvocationStack(err, this.getClass().getName());
        String msg = null; 
        if (err instanceof NoViableAltException) {
            NoViableAltException nvae = (NoViableAltException) err;
            // msg = "No viable alt; token=" + err.token.getText() +
            //     " (decision=" + nvae.decisionNumber +
            //     " state "+nvae.stateNumber+")" +
            //     " decision=<<" + nvae.grammarDecisionDescription + ">>";
            msg = "[line:" + err.line + ", col:" + err.charPositionInLine + "] " +
                "No viable alternative (token=" + err.token.getText() + ")" + " (stack=" + stack + ")";
        }
        else if (err instanceof MismatchedTokenException) {
            MismatchedTokenException mte = (MismatchedTokenException) err;
            String tokenName = (mte.expecting == Token.EOF) ? "EOF" : tokenNames[mte.expecting];
            msg = "[line:" + mte.line + ", col:" + mte.charPositionInLine + "] " +
                "Expecting " + tokenName +
                " (token=" + err.token.getText() + ")";
        }
        else if (err instanceof FailedPredicateException) {
            FailedPredicateException fpe = (FailedPredicateException) err;
            msg = "[line:" + fpe.line + ", col:" + fpe.charPositionInLine + "] " +
                fpe.predicateText +
                " (token=" + fpe.token.getText() + ")";
        }
        else if (err instanceof MismatchedSetException) {
            MismatchedSetException mse = (MismatchedSetException) err;
            msg = "[line:" + mse.line + ", col:" + mse.charPositionInLine + "] " +
                "Mismatched input (token=" + mse.token.getText() + ")";
        }
        else {
            msg = super.getErrorMessage(err, tokenNames); 
        }
        return msg;
    } 

    @Override
    public String getTokenErrorDisplay(Token t)
    {
        return t.toString();
    }

    private String predType(JSONObject pred)
    {
        return (String) pred.keys().next();
    }

    private String predField(JSONObject pred) throws JSONException
    {
        String type = (String) pred.keys().next();
        JSONObject fieldSpec = pred.getJSONObject(type);
        return (String) fieldSpec.keys().next();
    }

    private boolean verifyFacetType(final String field, final String expectedType)
    {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            Set<String> compatibleTypes = _compatibleFacetTypes.get(expectedType);
            return (expectedType.equals(facetInfo[0]) ||
                    "custom".equals(facetInfo[0]) ||
                    (compatibleTypes != null && compatibleTypes.contains(facetInfo[0])));
        }
        else {
            return true;
        }
    }

    private boolean verifyValueType(Object value, final String columnType)
    {
        if (value instanceof String &&
            !((String) value).isEmpty() &&
            ((String) value).matches("\\$[^$].*"))
        {
            // This "value" is a variable, return true always
            return true;
        }

        if (columnType.equals("long") || columnType.equals("aint") || columnType.equals("int") || columnType.equals("short")) {
            return !(value instanceof Float || value instanceof String || value instanceof Boolean);
        }
        else if (columnType.equals("float")  || columnType.equals("int") || columnType.equals("double")) {
            return !(value instanceof String || value instanceof Boolean);
        }
        else if (columnType.equals("string") || columnType.equals("char")) {
            return (value instanceof String);
        }
        else if (columnType.equals("boolean")) {
            return (value instanceof Boolean);
        }
        else if (columnType.isEmpty()) {
            // For a custom facet, the data type is unknown (empty
            // string).  We accept all value types here.
            return true;
        }
        else {
            return false;
        }
    }

    private boolean verifyFieldDataType(final String field, Object value)
        throws FailedPredicateException
    {
        String[] facetInfo = _facetInfoMap.get(field);

        if (value instanceof String &&
            !((String) value).isEmpty() &&
            ((String) value).matches("\\$[^$].*"))
        {
            // This "value" is a variable, return true always
            return true;
        }
        else if (value instanceof JSONArray)
        {
            try {
                if (facetInfo != null) {
                    String columnType = facetInfo[1];
                    for (int i = 0; i < ((JSONArray) value).length(); ++i) {
                        if (!verifyValueType(((JSONArray) value).get(i), columnType)) {
                            return false;
                        }
                    }
                }
                return true;
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "verifyFieldDataType", "JSONException: " + err.getMessage());
            }
        }
        else
        {
            if (facetInfo != null) {
                return verifyValueType(value, facetInfo[1]);
            }
            else {
                // Field is not a facet
                return true;
            }
        }
    }

    private boolean verifyFieldDataType(final String field, Object[] values)
    {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            String columnType = facetInfo[1];
            for (Object value: values) {
                if (!verifyValueType(value, columnType)) {
                    return false;
                }
            }
        }
        return true;
    }

    private void extractSelectionInfo(JSONObject where,
                                      JSONArray selections,
                                      JSONObject filter,
                                      JSONObject query) throws JSONException {

        JSONObject queryPred = where.optJSONObject("query");
        JSONArray andPreds = null;

        if (queryPred != null) {
            query.put("query", queryPred);
        }
        else if ((andPreds = where.optJSONArray("and")) != null) {
            JSONArray filter_list = new FastJSONArray();
            for (int i = 0; i < andPreds.length(); ++i) {
                JSONObject pred = andPreds.getJSONObject(i);
                queryPred = pred.optJSONObject("query");
                if (queryPred != null) {
                    if (!query.has("query")) {
                        query.put("query", queryPred);
                    }
                    else {
                        filter_list.put(pred);
                    }
                }
                else if (pred.has("and") || pred.has("or") || pred.has("isNull")) {
                    filter_list.put(pred);
                }
                else {
                    String[] facetInfo = _facetInfoMap.get(predField(pred));
                    if (facetInfo != null) {
                        if ("range".equals(predType(pred)) && !"range".equals(facetInfo[0])) {
                            filter_list.put(pred);
                        }
                        else {
                            selections.put(pred);
                        }
                    }
                    else {
                        filter_list.put(pred);
                    }
                }
            }
            if (filter_list.length() > 1) {
                filter.put("filter", new FastJSONObject().put("and", filter_list));
            }
            else if (filter_list.length() == 1) {
                filter.put("filter", filter_list.get(0));
            }
        }
        else if (where.has("or") || where.has("isNull")) {
            filter.put("filter", where);
        }
        else {
            String[] facetInfo = _facetInfoMap.get(predField(where));
            if (facetInfo != null) {
                if ("range".equals(predType(where)) && !"range".equals(facetInfo[0])) {
                    filter.put("filter", where);
                }
                else {
                    selections.put(where);
                }
            }
            else {
                filter.put("filter", where);
            }
        }
    }

    private int compareValues(Object v1, Object v2)
    {
        if (v1 instanceof String) {
            return ((String) v1).compareTo((String) v2);
        }
        else if (v1 instanceof Integer) {
            return ((Integer) v1).compareTo((Integer) v2);
        }
        else if (v1 instanceof Long) {
            return ((Long) v1).compareTo((Long) v2);
        }
        else if (v1 instanceof Float) {
            return ((Float) v1).compareTo((Float) v2);
        }
        return 0;
    }

    private Object[] getMax(Object value1, boolean include1, Object value2, boolean include2)
    {
        Object value;
        Boolean include;
        if (value1 == null) {
            value = value2;
            include = include2;
        }
        else if (value2 == null) {
            value = value1;
            include = include1;
        }
        else {
            int comp = compareValues(value1, value2);
            if (comp > 0) {
                value = value1;
                include = include1;
            }
            else if (comp == 0) {
                value = value1;
                include = (include1 && include2);
            }
            else {
                value = value2;
                include = include2;
            }
        }
        return new Object[]{value, include};
    }

    private Object[] getMin(Object value1, boolean include1, Object value2, boolean include2)
    {
        Object value;
        Boolean include;
        if (value1 == null) {
            value = value2;
            include = include2;
        }
        else if (value2 == null) {
            value = value1;
            include = include1;
        }
        else {
            int comp = compareValues(value1, value2);
            if (comp > 0) {
                value = value2;
                include = include2;
            }
            else if (comp == 0) {
                value = value1;
                include = (include1 && include2);
            }
            else {
                value = value1;
                include = include1;
            }
        }
        return new Object[]{value, include};
    }

    private void accumulateRangePred(TokenStream input, JSONObject fieldMap, JSONObject pred) 
        throws JSONException, RecognitionException
    {
        String field = predField(pred);
        if (!fieldMap.has(field)) {
            fieldMap.put(field, pred);
            return;
        }
        JSONObject oldRange = (JSONObject) fieldMap.get(field);
        JSONObject oldSpec = (JSONObject) ((JSONObject) oldRange.get("range")).get(field);
        Object oldFrom = oldSpec.opt("from");
        Object oldTo = oldSpec.opt("to");
        Boolean oldIncludeLower = oldSpec.optBoolean("include_lower", false);
        Boolean oldIncludeUpper = oldSpec.optBoolean("include_upper", false);

        JSONObject curSpec = (JSONObject) ((JSONObject) pred.get("range")).get(field);
        Object curFrom = curSpec.opt("from");
        Object curTo = curSpec.opt("to");
        Boolean curIncludeLower = curSpec.optBoolean("include_lower", false);
        Boolean curIncludeUpper = curSpec.optBoolean("include_upper", false);

        Object[] result = getMax(oldFrom, oldIncludeLower, curFrom, curIncludeLower);
        Object newFrom = result[0];
        Boolean newIncludeLower = (Boolean) result[1];
        result = getMin(oldTo, oldIncludeUpper, curTo, curIncludeUpper);
        Object newTo = result[0];
        Boolean newIncludeUpper = (Boolean) result[1];

        if (newFrom != null && newTo != null && !newFrom.toString().startsWith("$")&& !newTo.toString().startsWith("$")) {
            if (compareValues(newFrom, newTo) > 0 ||
                (compareValues(newFrom, newTo) == 0) && (!newIncludeLower || !newIncludeUpper)) {
                // This error is in general detected late, so the token
                // can be a little off, but hopefully the col index info
                // is good enough.
                throw new FailedPredicateException(input, "", "Inconsistent ranges detected for column: " + field);
            }
        }
        
        JSONObject newSpec = new FastJSONObject();
        if (newFrom != null) {
            newSpec.put("from", newFrom);
            newSpec.put("include_lower", newIncludeLower);
        }
        if (newTo != null) {
            newSpec.put("to", newTo);
            newSpec.put("include_upper", newIncludeUpper);
        }

        fieldMap.put(field, new FastJSONObject().put("range",
                                                     new FastJSONObject().put(field, newSpec)));
    }
    
    private void processRelevanceModelParam(TokenStream input,
                                            JSONObject json,
                                            Set<String> params,
                                            String typeName,
                                            final String varName)
        throws JSONException, RecognitionException
    {
        if (_facetInfoMap.containsKey(varName)) {
            throw new FailedPredicateException(input, "", "Facet name \"" + varName + "\" cannot be used as a relevance model parameter.");
        }

        if (_internalVarMap.containsKey(varName)) {
            throw new FailedPredicateException(input, "", "Internal variable \"" + varName + "\" cannot be used as a relevance model parameter.");
        }

        if (params.contains(varName)) {
            throw new FailedPredicateException(input, "", "Parameter name \"" + varName + "\" has already been used.");
        }

        if ("String".equals(typeName)) {
            typeName = "string";
        }

        JSONArray funcParams = json.optJSONArray("function_params");
        if (funcParams == null) {
            funcParams = new FastJSONArray();
            json.put("function_params", funcParams);
        }

        funcParams.put(varName);
        params.add(varName);

        JSONObject variables = json.optJSONObject("variables");
        if (variables == null) {
            variables = new FastJSONObject();
            json.put("variables", variables);
        }

        JSONArray varsWithSameType = variables.optJSONArray(typeName);
        if (varsWithSameType == null) {
            varsWithSameType = new FastJSONArray();
            variables.put(typeName, varsWithSameType);
        }
        varsWithSameType.put(varName);
    }

    // Check whether a variable is defined.
    private boolean verifyVariable(final String variable) {
        Iterator<Map<String, String>> itr = _symbolTable.descendingIterator();
        while (itr.hasNext()) {
            Map<String, String> scope = itr.next();
            if (scope.containsKey(variable)) {
                return true;
            }
        }
        return false;
    }
}

@rulecatch {
    catch (RecognitionException err) {
        throw err;
    }
}

fragment DIGIT : '0'..'9' ;
fragment ALPHA : 'a'..'z' | 'A'..'Z' ;

INTEGER : ('0' | '1'..'9' '0'..'9'*) INTEGER_TYPE_SUFFIX? ;
REAL : DIGIT+ '.' DIGIT* ;
LPAR : '(' ;
RPAR : ')' ;
COMMA : ',' ;
COLON : ':' ;
SEMI : ';' ;
EQUAL : '=' ;
GT : '>' ;
GTE : '>=' ;
LT : '<' ;
LTE : '<=';
NOT_EQUAL : '<>' ;

STRING_LITERAL
    :   ('"'
            { StringBuilder builder = new StringBuilder().appendCodePoint('"'); }
            ('"' '"'               { builder.appendCodePoint('"'); }
            | ch=~('"'|'\r'|'\n')  { builder.appendCodePoint(ch); }
            )*
         '"'
            { setText(builder.appendCodePoint('"').toString()); }
        )
    |
        ('\''
            { StringBuilder builder = new StringBuilder().appendCodePoint('\''); }
            ('\'' '\''             { builder.appendCodePoint('\''); }
            | ch=~('\''|'\r'|'\n') { builder.appendCodePoint(ch); }
            )*
         '\''
            { setText(builder.appendCodePoint('\'').toString()); }
        )
    ;

DATE
    :   DIGIT DIGIT DIGIT DIGIT ('-'|'/') DIGIT DIGIT ('-'|'/') DIGIT DIGIT 
    ;

TIME
    :
        DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT
    ;

//
// PQL Relevance model related
//

fragment HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;
fragment INTEGER_TYPE_SUFFIX: ('l' | 'L') ;
fragment EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;
fragment FLOAT_TYPE_SUFFIX : ('f'|'F'|'d'|'D') ;

fragment
ESCAPE_SEQUENCE
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESCAPE
    |   OCTAL_ESCAPE
    ;

fragment
UNICODE_ESCAPE
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

fragment
OCTAL_ESCAPE
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

HEX_LITERAL : '0' ('x'|'X') HEX_DIGIT+ INTEGER_TYPE_SUFFIX? ;
OCTAL_LITERAL : '0' ('0'..'7')+ INTEGER_TYPE_SUFFIX? ;

FLOATING_POINT_LITERAL
    :   REAL EXPONENT? FLOAT_TYPE_SUFFIX?
    |   '.' DIGIT+ EXPONENT? FLOAT_TYPE_SUFFIX?
    |   DIGIT+ EXPONENT FLOAT_TYPE_SUFFIX?
    |   DIGIT+ FLOAT_TYPE_SUFFIX
    ;

CHARACTER_LITERAL
    :   '\'' ( ESCAPE_SEQUENCE | ~('\''|'\\') ) '\''
    ;


//
// PQL Keywords
//

ALL : ('A'|'a')('L'|'l')('L'|'l') ;
AFTER : ('A'|'a')('F'|'f')('T'|'t')('E'|'e')('R'|'r') ;
AGAINST : ('A'|'a')('G'|'g')('A'|'a')('I'|'i')('N'|'n')('S'|'s')('T'|'t') ;
AGO : ('A'|'a')('G'|'g')('O'|'o') ;
AND : ('A'|'a')('N'|'n')('D'|'d') ;
AS : ('A'|'a')('S'|'s') ;
ASC : ('A'|'a')('S'|'s')('C'|'c') ;
BEFORE : ('B'|'b')('E'|'e')('F'|'f')('O'|'o')('R'|'r')('E'|'e') ;
BEGIN : ('B'|'b')('E'|'e')('G'|'g')('I'|'i')('N'|'n') ;
BETWEEN : ('B'|'b')('E'|'e')('T'|'t')('W'|'w')('E'|'e')('E'|'e')('N'|'n') ;
BOOLEAN : ('B'|'b')('O'|'o')('O'|'o')('L'|'l')('E'|'e')('A'|'a')('N'|'n') ;
BROWSE : ('B'|'b')('R'|'r')('O'|'o')('W'|'w')('S'|'s')('E'|'e') ;
BY : ('B'|'b')('Y'|'y') ;
BYTE : ('B'|'b')('Y'|'y')('T'|'t')('E'|'e') ;
BYTEARRAY : ('B'|'b')('Y'|'y')('T'|'t')('E'|'e')('A'|'a')('R'|'r')('R'|'r')('A'|'a')('Y'|'y') ;
CONTAINS : ('C'|'c')('O'|'o')('N'|'n')('T'|'t')('A'|'a')('I'|'i')('N'|'n')('S'|'s') ;
DEFINED : ('D'|'d')('E'|'e')('F'|'f')('I'|'i')('N'|'n')('E'|'e')('D'|'d') ;
DESC : ('D'|'d')('E'|'e')('S'|'s')('C'|'c') ;
DESCRIBE : ('D'|'d')('E'|'e')('S'|'s')('C'|'c')('R'|'r')('I'|'i')('B'|'b')('E'|'e') ;
DISTINCT : ('D'|'d')('I'|'i')('S'|'s')('T'|'t')('I'|'i')('N'|'n')('C'|'c')('T'|'t') ;
DOUBLE : ('D'|'d')('O'|'o')('U'|'u')('B'|'b')('L'|'l')('E'|'e') ;
EMPTY : ('E'|'e')('M'|'m')('P'|'p')('T'|'t')('Y'|'y') ;
ELSE : ('E'|'e')('L'|'l')('S'|'s')('E'|'e') ;
END : ('E'|'e')('N'|'n')('D'|'d') ;
EXCEPT : ('E'|'e')('X'|'x')('C'|'c')('E'|'e')('P'|'p')('T'|'t') ;
EXECUTE : ('E'|'e')('X'|'x')('E'|'e')('C'|'c')('U'|'u')('T'|'t')('E'|'e') ;
FACET : ('F'|'f')('A'|'a')('C'|'c')('E'|'e')('T'|'t') ;
FALSE : ('F'|'f')('A'|'a')('L'|'l')('S'|'s')('E'|'e') ;
FETCHING : ('F'|'f')('E'|'e')('T'|'t')('C'|'c')('H'|'h')('I'|'i')('N'|'n')('G'|'g') ;
FROM : ('F'|'f')('R'|'r')('O'|'o')('M'|'m') ;
GROUP : ('G'|'g')('R'|'r')('O'|'o')('U'|'u')('P'|'p') ;
GIVEN : ('G'|'g')('I'|'i')('V'|'v')('E'|'e')('N'|'n') ;
HITS : ('H'|'h')('I'|'i')('T'|'t')('S'|'s') ;
IN : ('I'|'i')('N'|'n') ;
INT : ('I'|'i')('N'|'n')('T'|'t') ;
IS : ('I'|'i')('S'|'s') ;
LAST : ('L'|'l')('A'|'a')('S'|'s')('T'|'t') ;
LIKE : ('L'|'l')('I'|'i')('K'|'k')('E'|'e') ;
LIMIT : ('L'|'l')('I'|'i')('M'|'m')('I'|'i')('T'|'t') ;
LONG : ('L'|'l')('O'|'o')('N'|'n')('G'|'g') ;
MATCH : ('M'|'m')('A'|'a')('T'|'t')('C'|'c')('H'|'h') ;
MODEL : ('M'|'m')('O'|'o')('D'|'d')('E'|'e')('L'|'l') ;
NOT : ('N'|'n')('O'|'o')('T'|'t') ;
NOW : ('N'|'n')('O'|'o')('W'|'w') ;
NULL : ('N'|'n')('U'|'u')('L'|'l')('L'|'l') ;
OR : ('O'|'o')('R'|'r') ;
ORDER : ('O'|'o')('R'|'r')('D'|'d')('E'|'e')('R'|'r') ;
PARAM : ('P'|'p')('A'|'a')('R'|'r')('A'|'a')('M'|'m') ;
QUERY : ('Q'|'q')('U'|'u')('E'|'e')('R'|'r')('Y'|'y') ;
ROUTE : ('R'|'r')('O'|'o')('U'|'u')('T'|'t')('E'|'e') ;
RELEVANCE : ('R'|'r')('E'|'e')('L'|'l')('E'|'e')('V'|'v')('A'|'a')('N'|'n')('C'|'c')('E'|'e') ;
SELECT : ('S'|'s')('E'|'e')('L'|'l')('E'|'e')('C'|'c')('T'|'t') ;
SINCE : ('S'|'s')('I'|'i')('N'|'n')('C'|'c')('E'|'e') ;
STORED : ('S'|'s')('T'|'t')('O'|'o')('R'|'r')('E'|'e')('D'|'d') ;
STRING : ('S'|'s')('T'|'t')('R'|'r')('I'|'i')('N'|'n')('G'|'g') ;
TOP : ('T'|'t')('O'|'o')('P'|'p') ;
TRUE : ('T'|'t')('R'|'r')('U'|'u')('E'|'e') ;
USING : ('U'|'u')('S'|'s')('I'|'i')('N'|'n')('G'|'g') ;
VALUE : ('V'|'v')('A'|'a')('L'|'l')('U'|'u')('E'|'e') ;
WHERE : ('W'|'w')('H'|'h')('E'|'e')('R'|'r')('E'|'e') ;
WITH : ('W'|'w')('I'|'i')('T'|'t')('H'|'h') ;

WEEKS : ('W'|'w')('E'|'e')('E'|'e')('K'|'k')('S'|'s')? ;
DAYS : ('D'|'d')('A'|'a')('Y'|'y')('S'|'s')? ;
HOURS : ('H'|'h')('O'|'o')('U'|'u')('R'|'r')('S'|'s')? ;
MINUTES : ('M'|'m')('I'|'i')('N'|'n')('U'|'u')('T'|'t')('E'|'e')('S'|'s')? ;
MINS : ('M'|'m')('I'|'i')('N'|'n')('S'|'s')? ;
SECONDS : ('S'|'s')('E'|'e')('C'|'c')('O'|'o')('N'|'n')('D'|'d')('S'|'s')? ;
SECS : ('S'|'s')('E'|'e')('C'|'c')('S'|'s')? ;
MILLISECONDS : ('M'|'m')('I'|'i')('L'|'l')('L'|'l')('I'|'i')('S'|'s')('E'|'e')('C'|'c')('O'|'o')('N'|'n')('D'|'d')('S'|'s')? ;
MSECS : ('M'|'m')('S'|'s')('E'|'e')('C'|'c')('S'|'s')? ;

FAST_UTIL_DATA_TYPE
    :   'IntOpenHashSet'
    |   'FloatOpenHashSet'
    |   'DoubleOpenHashSet'
    |   'LongOpenHashSet'
    |   'ObjectOpenHashSet'
    |   'Int2IntOpenHashMap'
    |   'Int2FloatOpenHashMap'
    |   'Int2DoubleOpenHashMap'
    |   'Int2LongOpenHashMap'
    |   'Int2ObjectOpenHashMap'
    |   'Object2IntOpenHashMap'
    |   'Object2FloatOpenHashMap'
    |   'Object2DoubleOpenHashMap'
    |   'Object2LongOpenHashMap'
    |   'Object2ObjectOpenHashMap'
    ;

// Have to define this after the keywords?
IDENT : (ALPHA | '_') (ALPHA | DIGIT | '-' | '_')* ;
VARIABLE : '$' (ALPHA | DIGIT | '_')+ ;

WS : ( ' ' | '\t' | '\r' | '\n' )+ { $channel = HIDDEN; };

COMMENT
    : '/*' .* '*/' { $channel = HIDDEN; }
    ;

LINE_COMMENT
    : '--' ~('\n'|'\r')* '\r'? '\n' { $channel = HIDDEN; }
    ;

// ***************** parser rules:

statement returns [Object json]
    :   (   select_stmt { $json = $select_stmt.json; }
        |   describe_stmt
        )   SEMI? EOF
    ;

select_stmt returns [Object json]
@init {
    _now = System.currentTimeMillis();
    _variables = new HashSet<String>();
    boolean seenOrderBy = false;
    boolean seenLimit = false;
    boolean seenGroupBy = false;
     boolean seenExecuteMapReduce = false;
    boolean seenDistinct = false;
    boolean seenBrowseBy = false;
    boolean seenFetchStored = false;
    boolean seenRouteBy = false;
    boolean seenRelevanceModel = false;
}
    :   SELECT ('*' | cols=selection_list)
        (FROM (collection_id=IDENT | collection_str=STRING_LITERAL))?
        w=where?
        given=given_clause?
        (   order_by = order_by_clause 
            { 
                if (seenOrderBy) {
                    throw new FailedPredicateException(input, "select_stmt", "ORDER BY clause can only appear once.");
                }
                else {
                    seenOrderBy = true;
                }
            }
        |   limit = limit_clause
            { 
                if (seenLimit) {
                    throw new FailedPredicateException(input, "select_stmt", "LIMIT clause can only appear once.");
                }
                else {
                    seenLimit = true;
                }
            }
        |   group_by = group_by_clause
            { 
                if (seenGroupBy) {
                    throw new FailedPredicateException(input, "select_stmt", "GROUP BY clause can only appear once.");
                }
                else {
                    seenGroupBy = true;
                }
            }
        |   distinct = distinct_clause
            { 
                if (seenDistinct) {
                    throw new FailedPredicateException(input, "select_stmt", "DISTINCT clause can only appear once.");
                }
                else {
                    seenDistinct = true;
                }
            }
        |   executeMapReduce = execute_clause
            { 
                if (seenExecuteMapReduce) {
                    throw new FailedPredicateException(input, "select_stmt", "EXECUTE clause can only appear once.");
                }
                else {
                    seenExecuteMapReduce = true;
                }
            }
        |   browse_by = browse_by_clause
            { 
                if (seenBrowseBy) {
                    throw new FailedPredicateException(input, "select_stmt", "BROWSE BY clause can only appear once.");
                }
                else {
                    seenBrowseBy = true;
                }
            }
        |   fetch_stored = fetching_stored_clause
            { 
                if (seenFetchStored) {
                    throw new FailedPredicateException(input, "select_stmt", "FETCHING STORED clause can only appear once.");
                }
                else {
                    seenFetchStored = true;
                }
            }
        |   route_param = route_by_clause
            {
                if (seenRouteBy) {
                    throw new FailedPredicateException(input, "select_stmt", "ROUTE BY clause can only appear once.");
                }
                else {
                    seenRouteBy = true;
                }
            }
        |   rel_model = relevance_model_clause
            {
                if (seenRelevanceModel) {
                    throw new FailedPredicateException(input, "select_stmt", "USING RELEVANCE MODEL clause can only appear once.");
                }
                else {
                    seenRelevanceModel = true;
                }
            }

        )*
        {
            JSONObject jsonObj = new FastJSONObject();
            JSONArray selections = new FastJSONArray();
            JSONObject filter = new FastJSONObject();
            JSONObject query = new FastJSONObject();

            try {
                JSONObject metaData = new FastJSONObject();
                if (cols == null) {
                    metaData.put("select_list", new FastJSONArray().put("*"));
                }
                else {
                   metaData.put("select_list", $cols.json);
                   if ($cols.fetchStored) {
                       jsonObj.put("fetchStored", true);
                   }
                }

                if (_variables.size() > 0)
                {
                    metaData.put("variables", new FastJSONArray(_variables));
                }

                jsonObj.put("meta", metaData);

                if (collection_id != null) {
                    jsonObj.put("collection", $collection_id.text);
                }
                else if(collection_str != null) {
                    String orig = $collection_str.text;
                    jsonObj.put("collection", orig.substring(1, orig.length() - 1));
                }

                if (order_by != null) {
                    if ($order_by.isRelevance) {
                        jsonObj.put("sort", "relevance");
                    }
                    else {
                        jsonObj.put("sort", $order_by.json);
                    }
                }
                if (limit != null) {
                    jsonObj.put("from", $limit.offset);
                    jsonObj.put("size", $limit.count);
                }
                if (group_by != null) {
                    jsonObj.put("groupBy", $group_by.json);
                    
                }  
                List<Pair<String, String>> aggregateFunctions = null;
                 if (cols != null) {
                    aggregateFunctions = $cols.aggregationFunctions;
                 }
                
                
                if (distinct != null) {
                    jsonObj.put("distinct", $distinct.json);
                }
                if (browse_by != null) {
                    jsonObj.put("facets", $browse_by.json);
                }
                if (executeMapReduce != null) {
                   if (group_by != null) {
                      PQLParserUtils.decorateWithMapReduce(jsonObj, $cols.aggregationFunctions, $group_by.json, $executeMapReduce.functionName, $executeMapReduce.properties);
                   } else {
                      PQLParserUtils.decorateWithMapReduce(jsonObj, $cols.aggregationFunctions, null, $executeMapReduce.functionName, $executeMapReduce.properties);
                   }
                } else {
                   if (group_by != null) {
                      PQLParserUtils.decorateWithMapReduce(jsonObj, $cols.aggregationFunctions, $group_by.json, null, null);
                   } else {
                      PQLParserUtils.decorateWithMapReduce(jsonObj, $cols.aggregationFunctions, null, null, null);
                   }
                }
                if (fetch_stored != null) {
                    if (!$fetch_stored.val && (cols != null && $cols.fetchStored)) {
                        throw new FailedPredicateException(input, 
                                                           "select_stmt",
                                                           "FETCHING STORED cannot be false when _srcdata is selected.");
                    }
                    else if ($fetch_stored.val) {
                        // Default is false
                        jsonObj.put("fetchStored", $fetch_stored.val);
                    }
                }
                if (route_param != null) {
                    jsonObj.put("routeParam", $route_param.val);
                }

                if (w != null) {
                    extractSelectionInfo((JSONObject) $w.json, selections, filter, query);
                    JSONObject queryPred = query.optJSONObject("query");
                    if (queryPred != null) {
                        jsonObj.put("query", queryPred);
                    }
                    if (selections.length() > 0) {
                        jsonObj.put("selections", selections);
                    }
                    JSONObject f = filter.optJSONObject("filter");
                    if (f != null) {
                        jsonObj.put("filter", f);
                    }
                }

                if (given != null) {
                    jsonObj.put("facetInit", $given.json);
                }

                if (rel_model != null) {
                    JSONObject queryPred = jsonObj.optJSONObject("query");
                    if (queryPred == null) {
                        queryPred = new FastJSONObject().put("query_string",
                                                             new FastJSONObject().put("query", "")
                                                                                 .put("relevance", $rel_model.json));
                        jsonObj.put("query", queryPred);
                    }
                }
                
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "select_stmt", "JSONException: " + err.getMessage());
            }

            $json = jsonObj;
        }
        // -> ^(SELECT column_name_list IDENT where?)
    ;

describe_stmt
    :   DESCRIBE (IDENT | STRING_LITERAL)
    ;

selection_list returns [boolean fetchStored, JSONArray json, List<Pair<String, String>> aggregationFunctions]
@init {
    $fetchStored = false;
    $json = new FastJSONArray();
    $aggregationFunctions = new ArrayList<Pair<String, String>>();
    }
    :    (col=column_name
        {
            String colName = $col.text;
            if (colName != null) {
                $json.put($col.text); 
                if ("_srcdata".equals(colName) || colName.startsWith("_srcdata.")) {
                    $fetchStored = true;
                }
            }
        } | agrFunction=aggregation_function 
            {
               $aggregationFunctions.add(new Pair($agrFunction.function, $agrFunction.column));
            })
        (COMMA   (col=column_name
        {
            String colName = $col.text;
            if (colName != null) {
                $json.put($col.text); 
                if ("_srcdata".equals(colName) || colName.startsWith("_srcdata.")) {
                    $fetchStored = true;
                }
            }
        }|  agrFunction=aggregation_function 
            {
                $aggregationFunctions.add(new Pair($agrFunction.function, $agrFunction.column));
            }))*;

aggregation_function returns [String function, String column]
 :   (id=function_name LPAR (columnVar=column_name | '*') RPAR) {
    $function= $id.text;    
    if (columnVar != null) {
        $column = $columnVar.text;
    } else {
        $column = ""; 
    }    
 };

column_name returns [String text]
@init {
    StringBuilder builder = new StringBuilder();
}
    :   (id=IDENT | str=STRING_LITERAL)
        {
            if (id != null) {
                builder.append($id.text);
            }
            else {
                String orig = $str.text;
                builder.append(orig.substring(1, orig.length() - 1));
            }
        }
        ('.' (id2=IDENT | str2=STRING_LITERAL)
        {
            builder.append(".");
            if (id2 != null) {
                builder.append($id2.text);
            }
            else {
                String orig = $str2.text;
                builder.append(orig.substring(1, orig.length() - 1));
            }
        }
        )*
        { $text = builder.toString(); }
    ;
function_name returns [String text] 

    :   (min= 'min'|colName=column_name)
        {
         if (min != null) {
           $text = "min";
         } else {
            $text = $colName.text; 
         }
        }
    ;
where returns [Object json]
    :   WHERE^ search_expr
        {
            $json = $search_expr.json;
        }
    ;

order_by_clause returns [boolean isRelevance, Object json]
    :   ORDER BY (RELEVANCE | sort_specs)
        {
            if ($RELEVANCE != null) {
                $isRelevance = true;
            }
            else {
                $isRelevance = false;
                $json = $sort_specs.json;
            }
        }
    ;

sort_specs returns [Object json]
@init {
    JSONArray sortArray = new FastJSONArray();
}
    :   sort=sort_spec
        {
            sortArray.put($sort.json);
        }
        (COMMA sort=sort_spec   // It's OK to use variable sort again here
            {
                sortArray.put($sort.json);
            }
        )*
        {
            $json = sortArray;
        }
    ;

sort_spec returns [JSONObject json]
    :   column_name (ordering=ASC | ordering=DESC)?
        {
            $json = new FastJSONObject();
            try {
                if ($ordering == null) {
                    $json.put($column_name.text, "asc");
                }
                else {
                    $json.put($column_name.text, $ordering.text.toLowerCase());
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "sort_spec", "JSONException: " + err.getMessage());
            }
        }
    ;

limit_clause returns [int offset, int count]
    :   LIMIT (n1=INTEGER COMMA)? n2=INTEGER
        {
            if (n1 != null) {
                $offset = Integer.parseInt($n1.text);
            }
            else {
                $offset = DEFAULT_REQUEST_OFFSET;
            }
            $count = Integer.parseInt($n2.text);
        }
    ;
comma_column_name_list returns [JSONArray json]
@init {
    $json = new FastJSONArray();
}
    :   col=column_name
        {
            String colName = $col.text;
            if (colName != null) {
                $json.put($col.text); 
            }
        }
        ((OR | COMMA) col=column_name
            {
                String colName = $col.text;
                if (colName != null) {
                    $json.put($col.text); 
                }
            }
        )*
    ;
or_column_name_list returns [JSONArray json]
@init {
    $json = new FastJSONArray();
}
    :   col=column_name
        {
            String colName = $col.text;
            if (colName != null) {
                $json.put($col.text); 
            }
        }
        (OR col=column_name
            {
                String colName = $col.text;
                if (colName != null) {
                    $json.put($col.text); 
                }
            }
        )*
    ;

group_by_clause returns [JSONObject json]
    :   GROUP BY comma_column_name_list (TOP top=INTEGER)?
        {
            $json = new FastJSONObject();
            try {
                JSONArray cols = $comma_column_name_list.json;
                /*for (int i = 0; i < cols.length(); ++i) {
                    String col = cols.getString(i);
                    String[] facetInfo = _facetInfoMap.get(col);
                    if (facetInfo != null && (facetInfo[0].equals("range") ||
                                              facetInfo[0].equals("multi") ||
                                              facetInfo[0].equals("path"))) {
                        throw new FailedPredicateException(input, 
                                                           "group_by_clause",
                                                           "Range/multi/path facet, \"" + col + "\", cannot be used in the GROUP BY clause.");
                    }
                }*/
                $json.put("columns", cols);
                if (top != null) {
                    $json.put("top", Integer.parseInt(top.getText()));
                }
                else {
                    $json.put("top", DEFAULT_REQUEST_MAX_PER_GROUP);
                }                    
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "group_by_clause", "JSONException: " + err.getMessage());
            }
        }
    ;

distinct_clause returns [JSONObject json]
    :   DISTINCT or_column_name_list
        {
            $json = new FastJSONObject();
            try {
                JSONArray cols = $or_column_name_list.json; 
                if (cols.length() > 1) {
                  throw new FailedPredicateException(input, 
                                                     "distinct_clause",
                                                     "DISTINCT only support a single column now.");
                }
                for (int i = 0; i < cols.length(); ++i) {
                    String col = cols.getString(i);
                    String[] facetInfo = _facetInfoMap.get(col);
                    if (facetInfo != null && (facetInfo[0].equals("range") ||
                                              facetInfo[0].equals("multi") ||
                                              facetInfo[0].equals("path"))) {
                        throw new FailedPredicateException(input, 
                                                           "distinct_clause",
                                                           "Range/multi/path facet, \"" + col + "\", cannot be used in the DISTINCT clause.");
                    }
                }
                $json.put("columns", cols);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "distinct_clause", "JSONException: " + err.getMessage());
            }
        }
    ;

browse_by_clause returns [JSONObject json]
@init {
    $json = new FastJSONObject();
}
    :   BROWSE BY f=facet_spec
        {
            try {
                $json.put($f.column, $f.spec);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "browse_by_clause", "JSONException: " + err.getMessage());
            }                    
        }
        (COMMA f=facet_spec
            {
                try {
                    $json.put($f.column, $f.spec);
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "browse_by_clause", "JSONException: " + err.getMessage());
                }
            }
        )*
    ;
execute_clause returns [String functionName, JSONObject properties]
@init {
    $properties = new FastJSONObject();
}
    :   EXECUTE LPAR funName=function_name
        {
            $functionName = $funName.text;
        }
        ((COMMA map=python_style_dict
            {
               $properties = $map.json;
               
            }
        ) | (
        (COMMA p=key_value_pair[KeyType.STRING_LITERAL]
            {
                try {
                    $properties.put($p.key, $p.val);
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "python_style_dict", "JSONException: " + err.getMessage());
                }
            }
        )*)
        )
        RPAR 
    ;
    
facet_spec returns [String column, JSONObject spec]
@init {
    boolean expand = false;
    int minhit = DEFAULT_FACET_MINHIT;
    int max = DEFAULT_FACET_MAXHIT;
    String orderBy = "hits";
}
    :   column_name
        (
            LPAR 
            (TRUE {expand = true;} | FALSE) COMMA
            n1=INTEGER COMMA
            n2=INTEGER COMMA
            (HITS | VALUE {orderBy = "val";})
            RPAR
        )*
        {
            $column = $column_name.text;
            if (n1 != null) {
                minhit = Integer.parseInt($n1.text);
            }
            if (n2 != null) {
                max = Integer.parseInt($n2.text);
            }
            try {
                $spec = new FastJSONObject().put("expand", expand)
                                            .put("minhit", minhit)
                                            .put("max", max)
                                            .put("order", orderBy);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "facet_spec", "JSONException: " + err.getMessage());
            }
        }
    ;

fetching_stored_clause returns [boolean val]
@init {
    $val = true;
}
    :   FETCHING STORED
        ((TRUE | FALSE {$val = false;})
        )*
    ;

route_by_clause returns [String val]
    :   ROUTE BY STRING_LITERAL 
        { 
            String orig = $STRING_LITERAL.text;
            $val = orig.substring(1, orig.length() - 1);
        }
    ;

search_expr returns [Object json]
@init {
    JSONArray array = new FastJSONArray();
}
    :   t=term_expr { array.put($t.json); }
        (OR t=term_expr { array.put($t.json); } )*
        {
            try {
                if (array.length() == 1) {
                    $json = array.get(0);
                }
                else {
                    $json = new FastJSONObject().put("or", array);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "search_expr", "JSONException: " + err.getMessage());
            }
        }
        -> {array.length() > 1}? ^(OR_PRED term_expr+)
        -> term_expr
    ;

term_expr returns [Object json]
@init {
    JSONArray array = new FastJSONArray();
}
    :   f=factor_expr { array.put($f.json); }
        (AND f=factor_expr { array.put($f.json); } )*
        {
            try {
                JSONArray newArray = new FastJSONArray();
                JSONObject fieldMap = new FastJSONObject();
                for (int i = 0; i < array.length(); ++i) {
                    JSONObject pred = (JSONObject) array.get(i);
                    if (!"range".equals(predType(pred))) {
                        newArray.put(pred);
                    }
                    else {
                        accumulateRangePred(input, fieldMap, pred);
                    }
                }
                Iterator<String> itr = fieldMap.keys();
                while (itr.hasNext()) {
                    newArray.put(fieldMap.get(itr.next()));
                }
                if (newArray.length() == 1) {
                    $json = newArray.get(0);
                }
                else {
                    $json = new FastJSONObject().put("and", newArray);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "term_expr", "JSONException: " + err.getMessage());
            }
        }
        -> { array.length() > 1}? ^(AND_PRED factor_expr+)
        -> factor_expr
    ;

factor_expr returns [Object json]
    :   predicate { $json = $predicate.json; }
    |   LPAR search_expr RPAR
        {$json = $search_expr.json;}
        -> search_expr
    ;

predicate returns [JSONObject json]
    :   in_predicate { $json = $in_predicate.json; }
    |   contains_all_predicate { $json = $contains_all_predicate.json; }
    |   equal_predicate { $json = $equal_predicate.json; }
    |   not_equal_predicate { $json = $not_equal_predicate.json; }
    |   query_predicate { $json = $query_predicate.json; }
    |   between_predicate { $json = $between_predicate.json; }
    |   range_predicate { $json = $range_predicate.json; }
    |   time_predicate { $json = $time_predicate.json; }
    |   match_predicate { $json = $match_predicate.json; }
    |   like_predicate { $json = $like_predicate.json; }
    |   null_predicate { $json = $null_predicate.json; }
    |   empty_predicate { $json = $empty_predicate.json; }
    ;

in_predicate returns [JSONObject json]
    :   column_name not=NOT? IN value_list except=except_clause? predicate_props?
        {
            String col = $column_name.text;
            String[] facetInfo = _facetInfoMap.get(col);

            if (facetInfo != null && facetInfo[0].equals("range")) {
                throw new FailedPredicateException(input, 
                                                   "in_predicate",
                                                   "Range facet \"" + col + "\" cannot be used in IN predicates.");
            }
            if (!verifyFieldDataType(col, $value_list.json)) {
                throw new FailedPredicateException(input,
                                                   "in_predicate",
                                                   "Value list for IN predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            if (except != null && !verifyFieldDataType(col, $except_clause.json)) {
                throw new FailedPredicateException(input,
                                                   "in_predicate",
                                                   "EXCEPT value list for IN predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            try {
                JSONObject dict = new FastJSONObject();
                dict.put("operator", "or");
                if (not == null) {
                    dict.put("values", $value_list.json);
                    if (except != null) {
                        dict.put("excludes", $except_clause.json);
                    }
                    else {
                        dict.put("excludes", new FastJSONArray());
                    }
                }
                else {
                    dict.put("excludes", $value_list.json);
                    if (except != null) {
                        dict.put("values", $except_clause.json);
                    }
                    else {
                        dict.put("values", new FastJSONArray());
                    }
                }
                if (_facetInfoMap.get(col) == null) {
                    dict.put("_noOptimize", true);
                }
                $json = new FastJSONObject().put("terms",
                                                 new FastJSONObject().put(col, dict));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "in_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(IN NOT? ^(column_name value_list) except_clause? predicate_props?)
    ;

empty_predicate returns [JSONObject json]
    :   value_list IS (NOT)? EMPTY
        {   
            try {
                JSONObject exp = new FastJSONObject();
                if ($NOT != null) {
                    JSONObject functionJSON = new FastJSONObject();
                    JSONArray params = new FastJSONArray();
                    params.put($value_list.json);
                    functionJSON.put("function", "length");
                    functionJSON.put("params", params);
                    exp.put("lvalue", functionJSON);
                    exp.put("operator", ">");
                    exp.put("rvalue", 0);
                    $json = new FastJSONObject().put("const_exp", exp);
                }
                else{
                    JSONObject functionJSON = new FastJSONObject();
                    JSONArray params = new FastJSONArray();
                    params.put($value_list.json);
                    functionJSON.put("function", "length");
                    functionJSON.put("params", params);
                    exp.put("lvalue", functionJSON);
                    exp.put("operator", "==");
                    exp.put("rvalue", 0);
                    $json = new FastJSONObject().put("const_exp", exp);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "empty_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;
    
contains_all_predicate returns [JSONObject json]
    :   column_name CONTAINS ALL value_list except=except_clause? predicate_props? 
        {
            String col = $column_name.text;
            String[] facetInfo = _facetInfoMap.get(col);
            if (facetInfo != null && facetInfo[0].equals("range")) {
                throw new FailedPredicateException(input, 
                                                   "contains_all_predicate", 
                                                   "Range facet column \"" + col + "\" cannot be used in CONTAINS ALL predicates.");
            }
            if (!verifyFieldDataType(col, $value_list.json)) {
                throw new FailedPredicateException(input,
                                                   "contains_all_predicate",
                                                   "Value list for CONTAINS ALL predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            if (except != null && !verifyFieldDataType(col, $except_clause.json)) {
                throw new FailedPredicateException(input,
                                                   "contains_all_predicate",
                                                   "EXCEPT value list for CONTAINS ALL predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            try {
                JSONObject dict = new FastJSONObject();
                dict.put("operator", "and");
                dict.put("values", $value_list.json);
                if (except != null) {
                    dict.put("excludes", $except_clause.json);
                }
                else {
                    dict.put("excludes", new FastJSONArray());
                }
                if (_facetInfoMap.get(col) == null) {
                    dict.put("_noOptimize", true);
                }
                $json = new FastJSONObject().put("terms",
                                                 new FastJSONObject().put(col, dict));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "contains_all_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(CONTAINS ^(column_name value_list) except_clause? predicate_props?)
    ;

equal_predicate returns [JSONObject json]
    :   column_name EQUAL value props=predicate_props?
        {
            String col = $column_name.text;
            if (!verifyFieldDataType(col, $value.val)) {
                throw new FailedPredicateException(input, 
                                                   "equal_predicate", 
                                                   "Incompatible data type was found in an EQUAL predicate for column \"" + col + "\".");
            }
            try {
                String[] facetInfo = _facetInfoMap.get(col);
                if (facetInfo != null && facetInfo[0].equals("range")) {
                    $json = new FastJSONObject().put("range",
                               new FastJSONObject().put(col,
                                   new FastJSONObject().put("from", $value.val)
                                                       .put("to", $value.val)
                                                       .put("include_lower", true)
                                                       .put("include_upper", true)));
                }
                else if (facetInfo != null && facetInfo[0].equals("path")) {
                    JSONObject valObj = new FastJSONObject();
                    valObj.put("value", $value.val);
                    if (props != null) {
                        JSONObject propsJson = $props.json;
                        Iterator<String> itr = propsJson.keys();
                        while (itr.hasNext()) {
                            String key = itr.next();
                            if (key.equals("strict") || key.equals("depth")) {
                                valObj.put(key, propsJson.get(key));
                            }
                            else {
                                throw new FailedPredicateException(input,
                                                                   "equal_predicate", 
                                                                   "Unsupported property was found in an EQUAL predicate for path facet column \"" + col + "\": " + key + ".");
                            }
                        }
                    }
                    $json = new FastJSONObject().put("path", new FastJSONObject().put(col, valObj));
                }
                else {
                    JSONObject valSpec = new FastJSONObject().put("value", $value.val);
                    if (_facetInfoMap.get(col) == null) {
                        valSpec.put("_noOptimize", true);
                    }
                    $json = new FastJSONObject().put("term", 
                                                     new FastJSONObject().put(col, valSpec));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "equal_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(EQUAL column_name value predicate_props?)
    ;

not_equal_predicate returns [JSONObject json]
    :   column_name NOT_EQUAL value predicate_props?
        {
            String col = $column_name.text;
            if (!verifyFieldDataType(col, $value.val)) {
                throw new FailedPredicateException(input, 
                                                   "not_equal_predicate", 
                                                   "Incompatible data type was found in a NOT EQUAL predicate for column \"" + col + "\".");
            }
            try {
                String[] facetInfo = _facetInfoMap.get(col);
                if (facetInfo != null && facetInfo[0].equals("range")) {
                    JSONObject left = new FastJSONObject().put("range",
                                                               new FastJSONObject().put(col,
                                                                                        new FastJSONObject().put("to", $value.val)
                                                                                                            .put("include_upper", false)));
                    JSONObject right = new FastJSONObject().put("range",
                                                                new FastJSONObject().put(col,
                                                                                         new FastJSONObject().put("from", $value.val)
                                                                                                             .put("include_lower", false)));
                    $json = new FastJSONObject().put("or", new FastJSONArray().put(left).put(right));
                }
                else if (facetInfo != null && facetInfo[0].equals("path")) {
                    throw new FailedPredicateException(input, 
                                                       "not_equal_predicate",
                                                       "NOT EQUAL predicate is not supported for path facets (column \"" + col + "\").");
                }
                else {
                    JSONObject valObj = new FastJSONObject();
                    valObj.put("operator", "or");
                    valObj.put("values", new FastJSONArray());
                    valObj.put("excludes", new FastJSONArray().put($value.val));
                    if (_facetInfoMap.get(col) == null) {
                        valObj.put("_noOptimize", true);
                    }
                    $json = new FastJSONObject().put("terms",
                                                     new FastJSONObject().put(col, valObj));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "not_equal_predicate", "JSONException: " + err.getMessage());
            }                                         
        }
        -> ^(NOT_EQUAL column_name value predicate_props?)
    ;

query_predicate returns [JSONObject json]
    :   QUERY IS STRING_LITERAL
        {
            try {
                String orig = $STRING_LITERAL.text;
                orig = orig.substring(1, orig.length() - 1);
                $json = new FastJSONObject().put("query",
                                                 new FastJSONObject().put("query_string",
                                                                          new FastJSONObject().put("query", orig)));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "query_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(QUERY STRING_LITERAL)
    ;

between_predicate returns [JSONObject json]
    :   column_name not=NOT? BETWEEN val1=value AND val2=value
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "between_predicate",
                                                   "Non-rangable facet column \"" + col + "\" cannot be used in BETWEEN predicates.");
            }

            if (!verifyFieldDataType(col, new Object[]{$val1.val, $val2.val})) {
                throw new FailedPredicateException(input,
                                                   "between_predicate", 
                                                   "Incompatible data type was found in a BETWEEN predicate for column \"" + col + "\".");
            }

            try {
                if (not == null) {
                    $json = new FastJSONObject().put("range",
                                                     new FastJSONObject().put(col,
                                                                              new FastJSONObject().put("from", $val1.val)
                                                                                                  .put("to", $val2.val)
                                                                                                  .put("include_lower", true)
                                                                                                  .put("include_upper", true)));
                }
                else {
                    JSONObject range1 = 
                        new FastJSONObject().put("range",
                                                 new FastJSONObject().put(col,
                                                                          new FastJSONObject().put("to", $val1.val)
                                                                                              .put("include_upper", false)));
                    JSONObject range2 = 
                        new FastJSONObject().put("range",
                                                 new FastJSONObject().put(col,
                                                                          new FastJSONObject().put("from", $val2.val)
                                                                                              .put("include_lower", false)));

                    $json = new FastJSONObject().put("or", new FastJSONArray().put(range1).put(range2));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "between_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(BETWEEN NOT? $val1 $val2)
    ;

range_predicate returns [JSONObject json]
    :   column_name (op=GT | op=GTE | op=LT | op=LTE) val=value
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "range_predicate",
                                                   "Non-rangable facet column \"" + col + "\" cannot be used in RANGE predicates.");
            }

            if (!verifyFieldDataType(col, $val.val)) {
                throw new FailedPredicateException(input,
                                                   "range_predicate", 
                                                   "Incompatible data type was found in a RANGE predicate for column \"" + col + "\".");
            }

            try {
                if ($op.text.charAt(0) == '>') {
                    $json = new FastJSONObject().put("range",
                                                     new FastJSONObject().put(col,
                                                                              new FastJSONObject().put("from", $val.val)
                                                                                                  .put("include_lower", ">=".equals($op.text))));
                }
                else {
                    $json = new FastJSONObject().put("range",
                                                     new FastJSONObject().put(col,
                                                                              new FastJSONObject().put("to", $val.val)
                                                                                                  .put("include_upper", "<=".equals($op.text))));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "range_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^($op column_name value)
    ;

time_predicate returns [JSONObject json]
    :   column_name (NOT)? IN LAST time_span
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "range_predicate",
                                                   "Non-rangable facet column \"" + col + "\" cannot be used in TIME predicates.");
            }

            try {
                if ($NOT == null) {
                    $json = new FastJSONObject().put("range",
                                                     new FastJSONObject().put(col,
                                                                              new FastJSONObject().put("from", $time_span.val)
                                                                                                  .put("include_lower", false)));
                }
                else {
                    $json = new FastJSONObject().put("range",
                                                     new FastJSONObject().put(col,
                                                                              new FastJSONObject().put("to", $time_span.val)
                                                                                                  .put("include_upper", true)));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "time_predicate", "JSONException: " + err.getMessage());
            }
        }
    |   column_name (NOT)? (since=SINCE | since=AFTER | before=BEFORE) time_expr
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "range_predicate",
                                                   "Non-rangable facet column \"" + col + "\" cannot be used in TIME predicates.");
            }

            try {
                if (since != null && $NOT == null ||
                    since == null && $NOT != null) {
                    $json = new FastJSONObject().put("range",
                                                     new FastJSONObject().put(col,
                                                                              new FastJSONObject().put("from", $time_expr.val)
                                                                                                  .put("include_lower", false)));
                }
                else {
                    $json = new FastJSONObject().put("range",
                                                     new FastJSONObject().put(col,
                                                                              new FastJSONObject().put("to", $time_expr.val)
                                                                                                  .put("include_upper", false)));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "time_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

time_span returns [long val]
    :   week=time_week_part? day=time_day_part? hour=time_hour_part? 
        minute=time_minute_part? second=time_second_part? msec=time_millisecond_part?
        {
            $val = 0;
            if (week != null) $val += $week.val;
            if (day != null) $val += $day.val;
            if (hour != null) $val += $hour.val;
            if (minute != null) $val += $minute.val;
            if (second != null) $val += $second.val;
            if (msec != null) $val += $msec.val;
            $val = _now - $val;
        }
    ;

time_week_part returns [long val]
    :   INTEGER WEEKS
        {
            $val = Integer.parseInt($INTEGER.text) * 7 * 24 * 60 * 60 * 1000L;
        }
    ;

time_day_part returns [long val]
    :   INTEGER DAYS
        {
          $val = Integer.parseInt($INTEGER.text) * 24 * 60 * 60 * 1000L;
        }
    ;

time_hour_part returns [long val]
    :   INTEGER HOURS
        {
          $val = Integer.parseInt($INTEGER.text) * 60 * 60 * 1000L;
        }
    ;

time_minute_part returns [long val]
    :   INTEGER (MINUTES | MINS)
        {
          $val = Integer.parseInt($INTEGER.text) * 60 * 1000L;
        }
    ;

time_second_part returns [long val]
    :   INTEGER (SECONDS | SECS)
        {
          $val = Integer.parseInt($INTEGER.text) * 1000L;
        }
    ;

time_millisecond_part returns [long val] 
    :   INTEGER (MILLISECONDS | MSECS)
        {
          $val = Integer.parseInt($INTEGER.text);
        }
    ;

time_expr returns [long val]
    :   time_span AGO
        {
            $val = $time_span.val;
        }
    |   date_time_string
        {
            $val = $date_time_string.val;
        }
    |   NOW
        {
            $val = _now;
        }
    ;

date_time_string returns [long val]
    :   DATE TIME?
        {
            SimpleDateFormat format;
            String dateTimeStr = $DATE.text;
            char separator = dateTimeStr.charAt(4);
            if ($TIME != null) {
                dateTimeStr = dateTimeStr + " " + $TIME.text;
            }
            int formatIdx = (separator == '-' ? 0 : 1);

            if ($TIME == null) {
                if (_format1[formatIdx] != null) {
                    format = _format1[formatIdx];
                }
                else {
                    format = _format1[formatIdx] = new SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd");
                }
            }
            else {
                if (_format2[formatIdx] != null) {
                    format = _format2[formatIdx];
                }
                else {
                    format = _format2[formatIdx] = new SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd HH:mm:ss");
                }
            }
            try {
                $val = format.parse(dateTimeStr).getTime();
                if (!dateTimeStr.equals(format.format($val))) {
                    throw new FailedPredicateException(input,
                                                       "date_time_string", 
                                                       "Date string contains invalid date/time: \"" + dateTimeStr + "\".");
                }
            }
            catch (ParseException err) {
                throw new FailedPredicateException(input,
                                                   "date_time_string", 
                                                   "ParseException happened for \"" + dateTimeStr + "\": " + 
                                                   err.getMessage() + ".");
            }
        }
    ;

match_predicate returns [JSONObject json]
    :   (NOT)? MATCH LPAR selection_list RPAR AGAINST LPAR STRING_LITERAL RPAR
        {
            try {
                JSONArray cols = $selection_list.json;
                for (int i = 0; i < cols.length(); ++i) {
                    String col = cols.getString(i);
                    String[] facetInfo = _facetInfoMap.get(col);
                    if (facetInfo != null && !facetInfo[1].equals("string")) {
                        throw new FailedPredicateException(input, 
                                                           "match_predicate", 
                                                           "Non-string type column \"" + col + "\" cannot be used in MATCH AGAINST predicates.");
                    }
                }

                String orig = $STRING_LITERAL.text;
                orig = orig.substring(1, orig.length() - 1);
                $json = new FastJSONObject().put("query",
                                                 new FastJSONObject().put("query_string",
                                                                          new FastJSONObject().put("fields", cols)
                                                                                              .put("query", orig)));
                if ($NOT != null) {
                    $json = new FastJSONObject().put("bool",
                                                     new FastJSONObject().put("must_not", $json));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "match_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

like_predicate returns [JSONObject json]
    :   column_name (NOT)? LIKE STRING_LITERAL
        {
            String col = $column_name.text;
            String[] facetInfo = _facetInfoMap.get(col);
            if (facetInfo != null && !facetInfo[1].equals("string")) {
                throw new FailedPredicateException(input, 
                                                   "match_predicate", 
                                                   "Non-string type column \"" + col + "\" cannot be used in LIKE predicates.");
            }
            String orig = $STRING_LITERAL.text;
            orig = orig.substring(1, orig.length() - 1);
            String likeString = orig.replace('\%', '*').replace('_', '?');
            try {
                $json = new FastJSONObject().put("query",
                                                 new FastJSONObject().put("wildcard",
                                                                          new FastJSONObject().put(col, likeString)));
                if ($NOT != null) {
                    $json = new FastJSONObject().put("bool",
                                                     new FastJSONObject().put("must_not", $json));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "like_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

null_predicate returns [JSONObject json]
    :   column_name IS (NOT)? NULL
        {
            String col = $column_name.text;
            try {
                $json = new FastJSONObject().put("isNull", col);
                if ($NOT != null) {
                    $json = new FastJSONObject().put("bool",
                                                     new FastJSONObject().put("must_not", $json));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "null_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

non_variable_value_list returns [JSONArray json]
@init {
    $json = new FastJSONArray();
}
    :   LPAR v=value
        {
            $json.put($v.val);
        }
        (COMMA v=value
            {
                $json.put($v.val);
            }
        )* RPAR
    |   LPAR RPAR
    ;

python_style_list returns [JSONArray json]
@init {
    $json = new FastJSONArray();
}
    :   '[' v=python_style_value?
        {
            $json.put($v.val);
        }
        (COMMA v=python_style_value
            {
                $json.put($v.val);
            }
        )* ']'
    ;

python_style_dict returns [JSONObject json]
@init {
    $json = new FastJSONObject();
}
    :   '{''}'        |
        '{' p=key_value_pair[KeyType.STRING_LITERAL]
        {
            try {
                $json.put($p.key, $p.val);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "python_style_dict", "JSONException: " + err.getMessage());
            }
        }
        (COMMA p=key_value_pair[KeyType.STRING_LITERAL]
            {
                try {
                    $json.put($p.key, $p.val);
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "python_style_dict", "JSONException: " + err.getMessage());
                }
            }
        )* '}'
    ;

python_style_value returns [Object val]
    :   value { $val = $value.val; }
    |   python_style_list { $val = $python_style_list.json; }
    |   python_style_dict { $val = $python_style_dict.json; }
    ;

value_list returns [Object json]
    :   non_variable_value_list
        {
            $json = $non_variable_value_list.json;
        }
    |   VARIABLE
        {
            $json = $VARIABLE.text;
            _variables.add(($VARIABLE.text).substring(1));
        }
    ;

value returns [Object val]
    :   numeric { $val = $numeric.val; }
    |   STRING_LITERAL
        {
            String orig = $STRING_LITERAL.text;
            orig = orig.substring(1, orig.length() - 1);
            $val = orig;
        }
    |   TRUE { $val = true; }
    |   FALSE { $val = false; }
    |   VARIABLE
        {
            $val = $VARIABLE.text;
            _variables.add(($VARIABLE.text).substring(1));
        }
    ;

numeric returns [Object val]
    :   time_expr { $val = $time_expr.val; }
    |   INTEGER {
            try {
                $val = Long.parseLong($INTEGER.text);
            }
            catch (NumberFormatException err) {
                throw new FailedPredicateException(input, "numeric", "Hit NumberFormatException: " + err.getMessage());
            }
        }
    |   REAL {
            try {
                $val = Float.parseFloat($REAL.text);
            }
            catch (NumberFormatException err) {
                throw new FailedPredicateException(input, "numeric", "Hit NumberFormatException: " + err.getMessage());
            }
        }
    ;

except_clause returns [Object json]
    :   EXCEPT^ value_list
        {
            $json = $value_list.json;
        }
    ;
  
predicate_props returns [JSONObject json]
    :   WITH^ prop_list[KeyType.STRING_LITERAL]
        {
            $json = $prop_list.json;
        }
    ;

prop_list[KeyType keyType] returns [JSONObject json]
@init {
    $json = new FastJSONObject();
}
    :   LPAR p=key_value_pair[keyType]
        {
            try {
                $json.put($p.key, $p.val);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "prop_list", "JSONException: " + err.getMessage());
            }
        }
        (COMMA p=key_value_pair[keyType]
            {
                try {
                    $json.put($p.key, $p.val);
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "prop_list", "JSONException: " + err.getMessage());
                }
            }
        )* RPAR
    ;

key_value_pair[KeyType keyType] returns [String key, Object val]
scope {
    KeyType type
}
@init {
    $key_value_pair::type = keyType;
}
    :   ( { $key_value_pair::type == KeyType.STRING_LITERAL ||
            $key_value_pair::type == KeyType.STRING_LITERAL_AND_IDENT}?=> STRING_LITERAL
        | { $key_value_pair::type == KeyType.IDENT ||
            $key_value_pair::type == KeyType.STRING_LITERAL_AND_IDENT}?=> IDENT
        )
        COLON (v=value | vs=python_style_list | vd=python_style_dict)
        {
            if ($STRING_LITERAL != null) {
                String orig = $STRING_LITERAL.text;
                $key = orig.substring(1, orig.length() - 1);
            }
            else {
                $key = $IDENT.text;
            }
            if (v != null) {
                $val = $v.val;
            }
            else if (vs != null) {
                $val = $vs.json;
            }
            else {
                $val = $vd.json;
            }
        }
    ;

given_clause returns [JSONObject json]
    :   GIVEN FACET PARAM facet_param_list
        {
            $json = $facet_param_list.json;
        }
    ;


// =====================================================================
// Relevance model related
// =====================================================================

variable_declarators returns [JSONArray json]
@init {
    $json = new FastJSONArray();
}
    :   var1=variable_declarator
        {
            $json.put($var1.varName);
        }
        (COMMA var2=variable_declarator
            {
                $json.put($var2.varName);
            }
        )*
    ;

variable_declarator returns [String varName]
    :   variable_declarator_id ('=' variable_initializer)?
        {
            $varName = $variable_declarator_id.varName;
        }
    ;

variable_declarator_id returns [String varName]
    :   IDENT ('[' ']')*
        {
            $varName = $IDENT.text;
        }
    ;

variable_initializer
    :   array_initializer
    |   expression
    ;

array_initializer
    :   '{' (variable_initializer (',' variable_initializer)* (',')?)? '}'
    ;

type returns [String typeName]
    :   class_or_interface_type ('[' ']')*
        { $typeName = $class_or_interface_type.typeName; }
    |   primitive_type ('[' ']')*
        { $typeName = $primitive_type.text;}
    |   boxed_type ('[' ']')*
        { $typeName = $boxed_type.text;}
    |   limited_type ('[' ']')*
        { $typeName = $limited_type.text;}
    ;

class_or_interface_type returns [String typeName]
    :   FAST_UTIL_DATA_TYPE
        { $typeName = _fastutilTypeMap.get($FAST_UTIL_DATA_TYPE.text); }
    ;

type_arguments returns [String typeArgs]
@init {
    StringBuilder builder = new StringBuilder();
}
    :   '<'
        ta1=type_argument
        { builder.append($ta1.text); }
        (COMMA ta2=type_argument
            { builder.append("_").append($ta2.text); }
        )* '>'
        {
            $typeArgs = builder.toString();
        }
    ;

type_argument
    :   type
    |   '?' (('extends' | 'super') type)?
    ;

formal_parameters returns [JSONObject json]
    :   LPAR formal_parameter_decls RPAR
        {
            $json = $formal_parameter_decls.json;
        }
    ;

formal_parameter_decls returns [JSONObject json]
@init {
    $json = new FastJSONObject();
    Set<String> params = new HashSet<String>();
}
    :   decl=formal_parameter_decl
        {
            try {
                processRelevanceModelParam(input, $json, params, $decl.typeName, $decl.varName);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input,
                                                   "formal_parameter_decls",
                                                   "JSONException: " + err.getMessage());
            }
        }
        (COMMA decl=formal_parameter_decl
            {
                try {
                    processRelevanceModelParam(input, $json, params, $decl.typeName, $decl.varName);
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input,
                                                       "formal_parameter_decl",
                                                       "JSONException: " + err.getMessage());
                }
            }
        )*
    ;
    
formal_parameter_decl returns [String typeName, String varName]
    :   variable_modifiers type variable_declarator_id
        {
            $typeName = $type.typeName;
            $varName = $variable_declarator_id.varName;
        }
    ;

primitive_type
    :   { "boolean".equals(input.LT(1).getText()) }? BOOLEAN
    |   'char'
    |   { "byte".equals(input.LT(1).getText()) }? BYTE
    |   'short'
    |   { "int".equals(input.LT(1).getText()) }? INT
    |   { "long".equals(input.LT(1).getText()) }? LONG
    |   'float'
    |   { "double".equals(input.LT(1).getText()) }? DOUBLE
    ;

boxed_type
    :   { "Boolean".equals(input.LT(1).getText()) }? BOOLEAN
    |   'Character'
    |   { "Byte".equals(input.LT(1).getText()) }? BYTE
    |   'Short'
    |   'Integer'
    |   { "Long".equals(input.LT(1).getText()) }? LONG
    |   'Float'
    |   { "Double".equals(input.LT(1).getText()) }? DOUBLE
    ;

limited_type
    :   'String'
    |   'System'
    |   'Math'     
    ;

variable_modifier
    :   'final'
    ;

relevance_model returns [String functionBody, JSONObject json]
@init {
    _usedFacets = new HashSet<String>();
    _usedInternalVars = new HashSet<String>();
    _symbolTable = new LinkedList<Map<String, String>>();
    _currentScope = new HashMap<String, String>();
    _symbolTable.offerLast(_currentScope);
}
    :   DEFINED AS params=formal_parameters
        {
            try {
                JSONObject varParams = $params.json.optJSONObject("variables");
                if (varParams != null) {
                    Iterator<String> itr = varParams.keys();
                    while (itr.hasNext()) {
                        String key = (String) itr.next();
                        JSONArray vars = varParams.getJSONArray(key);
                        for (int i = 0; i < vars.length(); ++i) {
                            _currentScope.put(vars.getString(i), key);
                        }
                    }
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "relevance_model", "JSONException: " + err.getMessage());
            }
        }
        BEGIN model_block END
        {
            $functionBody = $model_block.text;
            $json = $params.json;

            // Append facets and internal variable to "function_params".
            try {
                JSONArray funcParams = $json.getJSONArray("function_params");

                JSONObject facets = new FastJSONObject();
                $json.put("facets", facets);

                for (String facet: _usedFacets) {
                    funcParams.put(facet);
                    String[] facetInfo = _facetInfoMap.get(facet);
                    String typeName = (facetInfo[0].equals("multi") ? "m" :
                                       (facetInfo[0].equals("weighted-multi") ? "wm" : "")
                                      )
                                      + _facetInfoMap.get(facet)[1];
                    JSONArray facetsWithSameType = facets.optJSONArray(typeName);
                    if (facetsWithSameType == null) {
                        facetsWithSameType = new FastJSONArray();
                        facets.put(typeName, facetsWithSameType);
                    }
                    facetsWithSameType.put(facet);
                }

                // Internal variables, like _NOW, do not need to be
                // included in "variables".
                for (String varName: _usedInternalVars) {
                    if( ! _internalStaticVarMap.containsKey(varName))
                        funcParams.put(varName);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input,
                                                   "formal_parameter_decl",
                                                   "JSONException: " + err.getMessage());
            }
        }
    ;

model_block
    :   block_statement+
    ;

block
    :   '{' 
        {
            _currentScope = new HashMap<String, String>();
            _symbolTable.offerLast(_currentScope);
        }
        block_statement* 
        {
            _symbolTable.pollLast();
            _currentScope = _symbolTable.peekLast();
        }
        '}'
    ;

block_statement
    :   local_variable_declaration_stmt
    |   java_statement
    ;

local_variable_declaration_stmt
    :   local_variable_declaration SEMI
    ;

local_variable_declaration
    :   variable_modifiers type variable_declarators
        {
            try {
                JSONArray vars = $variable_declarators.json;
                for (int i = 0; i < vars.length(); ++i) {
                    String var = vars.getString(i);
                    if (_facetInfoMap.containsKey(var)) {
                        throw new FailedPredicateException(input,
                                                           "local_variable_declaration",
                                                           "Facet name \"" + var + "\" cannot be used to declare a variable.");
                    }
                    else if (_internalVarMap.containsKey(var)) {
                        throw new FailedPredicateException(input,
                                                           "local_variable_declaration",
                                                           "Internal variable \"" + var + "\" cannot be re-used to declare another variable.");
                    }
                    else if (verifyVariable(var)) {
                        throw new FailedPredicateException(input,
                                                           "local_variable_declaration",
                                                           "Variable \"" + var + "\" is already defined.");
                    }
                    else {
                        _currentScope.put(var, $type.typeName);
                    }
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "local_variable_declaration", "JSONException: " + err.getMessage());
            }
        }
    ;

variable_modifiers
    :   variable_modifier*
    ;

java_statement
    :   block
    |   'if' par_expression java_statement (else_statement)?
    |   'for' LPAR
        {
            _currentScope = new HashMap<String, String>();
            _symbolTable.offerLast(_currentScope);
        }
        for_control RPAR java_statement
        {
            _symbolTable.pollLast();
            _currentScope = _symbolTable.peekLast();
        }
    |   'while' par_expression java_statement
    |   'do' java_statement 'while' par_expression SEMI
    |   'switch' par_expression '{' switch_block_statement_groups '}'
    |   'return' expression SEMI
    |   'break' IDENT? SEMI
    |   'continue' IDENT? SEMI
    |   SEMI
    |   statement_expression SEMI
    ;

else_statement
    :   { "else".equals(input.LT(1).getText()) }? ELSE java_statement
    ;

switch_block_statement_groups
    :   (switch_block_statement_group)*
    ;

switch_block_statement_group
    :   switch_label+ block_statement*
    ;

switch_label
    :   'case' constant_expression COLON
    |   'case' enum_constant_name COLON
    |   'default' COLON
    ;

for_control
options {k=3;}
    :   enhanced_for_control
    |   for_init? SEMI expression? SEMI for_update?
    ;

for_init
    :   local_variable_declaration
    |   expression_list
    ;

enhanced_for_control
    :   variable_modifiers type IDENT COLON expression
    ;

for_update
    :   expression_list
    ;

par_expression
    :   LPAR expression RPAR
    ;

expression_list
    :   expression (',' expression)*
    ;

statement_expression
    :   expression
    ;

constant_expression
    :   expression
    ;

enum_constant_name
    :   IDENT
    ;

expression
    :   conditional_expression (assignment_operator expression)?
    ;

assignment_operator
    :   '='
    |   '+='
    |   '-='
    |   '*='
    |   '/='
    |   '&='
    |   '|='
    |   '^='
    |   '%='
    |   ('<' '<' '=')=> t1='<' t2='<' t3='=' 
        { $t1.getLine() == $t2.getLine() &&
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() && 
          $t2.getLine() == $t3.getLine() && 
          $t2.getCharPositionInLine() + 1 == $t3.getCharPositionInLine() }?
    |   ('>' '>' '>' '=')=> t1='>' t2='>' t3='>' t4='='
        { $t1.getLine() == $t2.getLine() && 
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() &&
          $t2.getLine() == $t3.getLine() && 
          $t2.getCharPositionInLine() + 1 == $t3.getCharPositionInLine() &&
          $t3.getLine() == $t4.getLine() && 
          $t3.getCharPositionInLine() + 1 == $t4.getCharPositionInLine() }?
    |   ('>' '>' '=')=> t1='>' t2='>' t3='='
        { $t1.getLine() == $t2.getLine() && 
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() && 
          $t2.getLine() == $t3.getLine() && 
          $t2.getCharPositionInLine() + 1 == $t3.getCharPositionInLine() }?
    ;

conditional_expression
    :   conditional_or_expression ( '?' expression ':' expression )?
    ;

conditional_or_expression
    :   conditional_and_expression ( '||' conditional_and_expression )*
    ;

conditional_and_expression
    :   inclusive_or_expression ('&&' inclusive_or_expression )*
    ;

inclusive_or_expression
    :   exclusive_or_expression ('|' exclusive_or_expression )*
    ;

exclusive_or_expression
    :   and_expression ('^' and_expression )*
    ;

and_expression
    :   equality_expression ( '&' equality_expression )*
    ;

equality_expression
    :   instanceof_expression ( ('==' | '!=') instanceof_expression )*
    ;

instanceof_expression
    :   relational_expression ('instanceof' type)?
    ;

relational_expression
    :   shift_expression ( relational_op shift_expression )*
    ;

relational_op
    :   ('<' '=')=> t1='<' t2='=' 
        { $t1.getLine() == $t2.getLine() && 
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() }?
    |   ('>' '=')=> t1='>' t2='=' 
        { $t1.getLine() == $t2.getLine() && 
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() }?
    |   '<'
    |   '>'
    ;

shift_expression
    :   additive_expression ( shift_op additive_expression )*
    ;

shift_op
    :   ('<' '<')=> t1='<' t2='<' 
        { $t1.getLine() == $t2.getLine() && 
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() }?
    |   ('>' '>' '>')=> t1='>' t2='>' t3='>' 
        { $t1.getLine() == $t2.getLine() && 
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() &&
          $t2.getLine() == $t3.getLine() && 
          $t2.getCharPositionInLine() + 1 == $t3.getCharPositionInLine() }?
    |   ('>' '>')=> t1='>' t2='>'
        { $t1.getLine() == $t2.getLine() && 
          $t1.getCharPositionInLine() + 1 == $t2.getCharPositionInLine() }?
    ;

additive_expression
    :   multiplicative_expression ( ('+' | '-') multiplicative_expression )*
    ;

multiplicative_expression
    :   unary_expression ( ( '*' | '/' | '%' ) unary_expression )*
    ;
    
unary_expression
    :   '+' unary_expression
    |   '-' unary_expression
    |   '++' unary_expression
    |   '--' unary_expression
    |   unary_expression_not_plus_minus
    ;

unary_expression_not_plus_minus
    :   '~' unary_expression
    |   '!' unary_expression
    |   cast_expression
    |   primary selector* ('++'|'--')?
    ;

cast_expression
    :  '(' primitive_type ')' unary_expression
    |  '(' (type | expression) ')' unary_expression_not_plus_minus
    ;

primary
    :   par_expression
    |   literal   
    |   java_method identifier_suffix
    |   java_ident ('.' java_method)* identifier_suffix?
        {
            String var = $java_ident.text;
            if (_facetInfoMap.containsKey(var)) {
                _usedFacets.add(var);
            }
            else if (_internalVarMap.containsKey(var)) {
                _usedInternalVars.add(var);
            }
            else if (!_supportedClasses.contains(var) && !verifyVariable(var)) {
                throw new FailedPredicateException(input,
                                                   "primary",
                                                   "Variable or class \"" + var + "\" is not defined.");
            }
        }        
    ;

java_ident
    :   boxed_type
    |   limited_type
    |   IDENT
    ;

// Need to handle the conflicts of PQL keywords and common Java method
// names supported by PQL.
java_method
    :   { "contains".equals(input.LT(1).getText()) }? CONTAINS
    |   IDENT
    ;

identifier_suffix
    :   ('[' ']')+ '.' 'class'
    |   arguments
    |   '.' 'class'
    |   '.' 'this'
    |   '.' 'super' arguments
    ;

literal 
    :   integer_literal
    |   REAL
    |   FLOATING_POINT_LITERAL
    |   CHARACTER_LITERAL
    |   STRING_LITERAL
    |   boolean_literal
    |   { "null".equals(input.LT(1).getText()) }? NULL
    ;

integer_literal
    :   HEX_LITERAL
    |   OCTAL_LITERAL
    |   INTEGER
    ;

boolean_literal
    :   { "true".equals(input.LT(1).getText()) }? TRUE
    |   { "false".equals(input.LT(1).getText()) }? FALSE
    ;

selector
    :   '.' IDENT arguments?
    |   '.' 'this'
    |   '[' expression ']'
    ;

arguments
    :   '(' expression_list? ')'
    ;
    
relevance_model_clause returns [JSONObject json]
@init {
    $json = new FastJSONObject();
}
    :   USING RELEVANCE MODEL IDENT prop_list[KeyType.STRING_LITERAL_AND_IDENT] model=relevance_model?
        {
            try {
                if (model == null) {
                    $json.put("predefined_model", $IDENT.text);
                    $json.put("values", $prop_list.json);
                }
                else {
                    JSONObject modelInfo = $model.json;
                    JSONObject modelJson = new FastJSONObject();
                    modelJson.put("function", $model.functionBody);

                    JSONArray funcParams = modelInfo.optJSONArray("function_params");
                    if (funcParams != null) {
                        modelJson.put("function_params", funcParams);
                    }

                    JSONObject facets = modelInfo.optJSONObject("facets");
                    if (facets != null) {
                        modelJson.put("facets", facets);
                    }

                    JSONObject variables = modelInfo.optJSONObject("variables");
                    if (variables != null) {
                        modelJson.put("variables", variables);
                    }

                    $json.put("model", modelJson);
                    $json.put("values", $prop_list.json);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "relevance_model_clause",
                                                   "JSONException: " + err.getMessage());
            }
        }
    ;

facet_param_list returns [JSONObject json]
@init {
    $json = new FastJSONObject();
}
    :   p=facet_param
        {
            try {
                if (!$json.has($p.facet)) {
                    $json.put($p.facet, $p.param);
                }
                else {
                    JSONObject currentParam = (JSONObject) $json.get($p.facet);
                    String paramName = (String) $p.param.keys().next();
                    currentParam.put(paramName, $p.param.get(paramName));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "facet_param_list", "JSONException: " + err.getMessage());
            }
        }
        (COMMA p=facet_param
            {
                try {
                    if (!$json.has($p.facet)) {
                        $json.put($p.facet, $p.param);
                    }
                    else {
                        JSONObject currentParam = (JSONObject) $json.get($p.facet);
                        String paramName = (String) $p.param.keys().next();
                        currentParam.put(paramName, $p.param.get(paramName));
                    }
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "facet_param_list", "JSONException: " + err.getMessage());
                }
            }
        )*
    ;

facet_param returns [String facet, JSONObject param]
    :   LPAR column_name COMMA STRING_LITERAL COMMA facet_param_type COMMA (val=value | valList=non_variable_value_list) RPAR
        {
            $facet = $column_name.text; // XXX Check error here?
            try {
                Object valArray = null;
                if (val != null) {
                    String varName = $value.text;
                    if (varName.matches("\\$[^$].*")) {
                        // Here "value" is a variable.  In this case, it
                        // is REQUIRED that the variable should be
                        // replaced by a list, NOT a scalar value.
                        valArray = varName;
                    }
                    else {
                        valArray = new FastJSONArray().put(val.val);
                    }
                }
                else {
                    valArray = $valList.json;
                }

                String orig = $STRING_LITERAL.text;
                orig = orig.substring(1, orig.length() - 1);
                $param = new FastJSONObject().put(orig,
                                                  new FastJSONObject().put("type", $facet_param_type.paramType)
                                                                      .put("values", valArray));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "facet_param", "JSONException: " + err.getMessage());
            }
        }                                 
    ;

facet_param_type returns [String paramType]
    :   (t=BOOLEAN | t=INT | t=LONG | t=STRING | t=BYTEARRAY | t=DOUBLE) 
        {
            $paramType = $t.text;
        }
    ;

//
// The end of PQL.g
//
