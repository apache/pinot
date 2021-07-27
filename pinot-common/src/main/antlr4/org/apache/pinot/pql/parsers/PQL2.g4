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
grammar PQL2;

root: statement ';'? EOF;

statement: selectStatement;
selectStatement : SELECT topClause? outputColumns FROM tableName optionalClause* # Select;

optionalClause:
  whereClause       # Where
  | groupByClause   # GroupBy
  | havingClause    # Having
  | orderByClause   # OrderBy
  | topClause       # Top
  | limitClause     # Limit
  | optionListClause   # Options
  ;

outputColumns:
  '*'                                 # StarColumnList
  | outputColumnProjection (',' outputColumnProjection)*  # OutputColumnList;
outputColumnProjection:
  expression (AS (IDENTIFIER | STRING_LITERAL))?    # OutputColumn;

expression:
  IDENTIFIER                                    # Identifier
  | IDENTIFIER '.' IDENTIFIER                   # Identifier
  | literal                                     # Constant
  | '(' expression ')'                          # ExpressionParenthesisGroup
  | function '(' expressions? ')'               # FunctionCall
  | expression binaryMathOperator expression    # BinaryMathOp
  ;

expressions:
  expression (',' expression)*    # ExpressionList
  | '*'                           # StarExpression
  ;

binaryMathOperator: '+' | '-' | '*' | '/';

function: IDENTIFIER;
tableName: IDENTIFIER | IDENTIFIER '.' IDENTIFIER | STRING_LITERAL;

literal:
  STRING_LITERAL            # StringLiteral
  | INTEGER_LITERAL         # IntegerLiteral
  | FLOATING_POINT_LITERAL  # FloatingPointLiteral
  ;

whereClause: WHERE predicateList;

predicateList:
  predicate (booleanOperator predicate)*
  ;

predicate:
  '(' predicateList ')'                   # PredicateParenthesisGroup
  | comparisonClause                      # ComparisonPredicate
  | inClause                              # InPredicate
  | betweenClause                         # BetweenPredicate
  | isClause                              # IsPredicate
  | regexpLikeClause                      # RegexpLikePredicate
  | textMatchClause                       # TextMatchPredicate
  | likeClause                            # TextMatchPredicate
  | jsonMatchClause                       # JsonMatchPredicate
  ;

inClause:
  expression NOT? IN '(' literal (',' literal)* ')';

isClause:
  expression IS NOT? NULL;

comparisonClause:
  expression comparisonOperator expression;
comparisonOperator: '<' | '>' | '<>' | '<=' | '>=' | '=' | '!=';

betweenClause:
  expression BETWEEN expression AND expression;

regexpLikeClause:
  REGEXP_LIKE '(' expression ',' literal ')';

textMatchClause:
  TEXT_MATCH '(' expression ',' literal ')';

likeClause:
  expression LIKE  literal;

jsonMatchClause:
  JSON_MATCH '(' expression ',' literal ')';

booleanOperator: OR | AND;

groupByClause: GROUP BY groupByList;
groupByList: expression (',' expression)*;

havingClause: HAVING predicateList;

orderByClause: ORDER BY orderByList;
orderByList: orderByExpression (',' orderByExpression)*;
orderByExpression: expression ordering?;
ordering: DESC | ASC;

topClause: TOP INTEGER_LITERAL;

limitClause: LIMIT INTEGER_LITERAL (',' INTEGER_LITERAL)?;

optionListClause: OPTION '(' option (',' option)* ')';
option: expression ('=' expression)?;

// Keywords
AND: A N D;
AS: A S;
ASC : A S C;
BETWEEN: B E T W E E N;
BY: B Y;
DESC: D E S C;
FROM: F R O M;
GROUP: G R O U P;
HAVING: H A V I N G;
IN: I N;
IS: I S;
NULL: N U L L;
LIMIT: L I M I T;
NOT : N O T;
OR: O R;
REGEXP_LIKE: R E G E X P '_' L I K E;
TEXT_MATCH: T E X T '_' M A T C H;
ORDER: O R D E R;
SELECT: S E L E C T;
TOP: T O P;
WHERE: W H E R E;
OPTION: O P T I O N;
LIKE: L I K E;


WHITESPACE: [ \t\n]+ -> skip;

LINE_COMMENT: '--' ~[\r\n]* -> channel(HIDDEN);

IDENTIFIER: '$'?[A-Za-z_][A-Za-z0-9_-]* | '`' (~'`')+ '`';
STRING_LITERAL: '\'' ( ~'\'' | '\'\'')* '\'' | '"' (~'"' | '""')* '"';
INTEGER_LITERAL : SIGN? DIGIT+;
FLOATING_POINT_LITERAL : SIGN? DIGIT+ '.' DIGIT* | SIGN? DIGIT* '.' DIGIT+;

fragment SIGN: [+-];

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
