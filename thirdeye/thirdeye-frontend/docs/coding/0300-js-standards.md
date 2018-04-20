JavaScript Style & Best Practices
=================================

*A fork of a mostly reasonable approach to JavaScript*

Types
-----
### 1.1 Primitives

-   When you access a primitive type you work directly on its value.
-   `string`
-   `number`
-   `boolean`
-   `null`
-   `undefined`

```js
const foo = 1;
let bar = foo;

bar = 9;

console.log(foo, bar); // => 1, 9
```

### 1.2 Complex

-   When you access a complex type you work on a reference to its value.
-   `object`
-   `array`
-   `function`

```js
const foo = [1, 2];
const bar = foo;

bar[0] = 9;

console.log(foo[0], bar[0]); // => 9, 9
```

References
----------

### 2.1 Use of `const`

-   Use `const` for all of your references; avoid using `var`.

    > Why? This ensures that you can't reassign your references, which
    > can lead to bugs and difficult to comprehend code.

```js
// bad
var a = 1;
var b = 2;

// good
const a = 1;
const b = 2;
```

#### ESLint Rules

-   [prefer-const](http://eslint.org/docs/rules/prefer-const)
-   [no-const-assign](http://eslint.org/docs/rules/no-const-assign)

### 2.2 Use of `let`

-   If you must reassign references, use `let` instead of `var`.

    > Why? `let` is block-scoped rather than function-scoped like `var`.

```js
// bad
var count = 1;
if (true) {
  count += 1;
}

// good, use the let.
let count = 1;
if (true) {
  count += 1;
}
```

#### ESLint Rules

-   [no-var](http://eslint.org/docs/rules/no-var)

#### JSCS Rules

-   [disallowVar](http://jscs.info/rule/disallowVar)

### 2.3 Block Scoping

-   Note that both `let` and `const` are block-scoped.

```js
// const and let only exist in the blocks they are defined in.
{
  let a = 1;
  const b = 1;
}
console.log(a); // ReferenceError
console.log(b); // ReferenceError
```

Objects
-------
### 3.1 Creating Objects

-   Use the literal syntax for object creation.

    > Why? Object literal notation is more concise and saves an
    > unecessary function call (if not already optimized out by the
    > JavaScript engine).

```js
// bad
const item = new Object();

// good
const item = {};
```

#### ESLint Rules

-   [no-new-object](http://eslint.org/docs/rules/no-new-object)

### 3.2 Naming Properties

-   If your code will be executed in browsers in script context, don't
    use [reserved words](http://es5.github.io/#x7.6.1) as keys. Itâ€™s OK
    to use them in ES6 modules and server-side code. [See this issue for
    more info](https://github.com/airbnb/javascript/issues/61).

```js
// bad
const superman = {
  default: { clark: 'kent' },
  private: true,
};

// good
const superman = {
  defaults: { clark: 'kent' },
  hidden: true,
};
```

#### JSCS Rules

-   [disallowIdentiferNames](http://jscs.info/rule/disallowIdentifierNames)

### 3.3 Synonyms for Reserved Words

-   Use readable synonyms in place of reserved words.

```js
// bad
const superman = {
  class: 'alien',
};

// bad
const superman = {
  klass: 'alien',
};

// good
const superman = {
  type: 'alien',
};
```

#### JSCS Rules

-   [disallowIdentiferNames](http://jscs.info/rule/disallowIdentifierNames)

### 3.4 Creating Objects with Dynamic Property Names

-   Use computed property names when creating objects with dynamic
    property names.

    > Why? They allow you to define all the properties of an object in
    > one place.

```js
function getKey(k) {
  return `a key named ${k}`;
}

// bad
const obj = {
  id: 5,
  name: 'San Francisco',
};
obj[getKey('enabled')] = true;

// good
const obj = {
  id: 5,
  name: 'San Francisco',
  [getKey('enabled')]: true,
};
```

### 3.5 Creating Objects with Methods

-   Use object method shorthand.

```js
// bad
const atom = {
  value: 1,

  addValue: function (value) {
    return atom.value + value;
  },
};

// good
const atom = {
  value: 1,

  addValue(value) {
    return atom.value + value;
  },
};
```

#### ESLint Rules

-   [object-shorthand](http://eslint.org/docs/rules/object-shorthand)

#### JSCS Rules

-   [requireEnhancedObjectLiterals](http://jscs.info/rule/requireEnhancedObjectLiterals)

### 3.6 Property Value Shorthand

-   Use property value shorthand.

    > Why? It is shorter to write and descriptive.

```js
const lukeSkywalker = 'Luke Skywalker';

// bad
const obj = {
  lukeSkywalker: lukeSkywalker,
};

// good
const obj = {
  lukeSkywalker,
};
```

#### ESLint Rules

-   [object-shorthand](http://eslint.org/docs/rules/object-shorthand)

#### JSCS Rules

-   [requireEnhancedObjectLiterals](http://jscs.info/rule/requireEnhancedObjectLiterals)

### 3.7 Ordering Properties

-   Group your shorthand properties at the beginning of your object
    declaration. Group quoted properties at the end.

    > Why? It's easier to tell which properties are using the shorthand.

```js
const anakinSkywalker = 'Anakin Skywalker';
const lukeSkywalker = 'Luke Skywalker';

// bad
const obj = {
  episodeOne: 1,
  lukeSkywalker,
  'episode-three': 3,
  twoJediWalkIntoACantina: 2,
  'may-the-fourth': 4,
  anakinSkywalker,
};

// good
const obj = {
  lukeSkywalker,
  anakinSkywalker,
  episodeOne: 1,
  twoJediWalkIntoACantina: 2,
  'episode-three': 3,
  'may-the-fourth': 4,
};
```

### 3.8 Accessing Properties

-   Only quote properties that are invalid identifiers.

    > Why? In general we consider it subjectively easier to read. It
    > improves syntax highlighting, and is also more easily optimized by
    > many JS engines.

```js
// bad
const bad = {
  'foo': 3,
  'bar': 4,
  'data-blah': 5,
};

// good
const good = {
  foo: 3,
  bar: 4,
  'data-blah': 5,
};
```

#### ESLint Rules

-   [quote-props](http://eslint.org/docs/rules/quote-props)

#### JSCS Rules

-   [disallowQuotedKeysInObjects](http://jscs.info/rule/disallowQuotedKeysInObjects)

Arrays
------

### 4.1 Creating Arrays

-   Use the literal syntax for array creation.

    > Why not? For performance reasons, you might need to initialize an
    > array to a particular length.

```js
// bad
const items = new Array();

// okay (when needed for performance)
const items = new Array(size); // eslint-disable-line no-array-constructor

// best
const items = [];
```

#### ESLint Rules

-   [no-array-constructor](http://eslint.org/docs/rules/no-array-constructor)

### 4.2 Adding to Arrays

-   Use Array\#push instead of direct assignment to add items to an
    array.

```js
const someStack = [];

// bad
someStack[someStack.length] = 'abracadabra';

// good
someStack.push('abracadabra');
```

### 4.3 Copying Arrays

-   Use array spreads `...` to copy arrays.

    > Why not? If you're writing a performance critical piece of code,
    > using Array\#slice is marginally faster.

```js
// bad
const len = items.length;
const itemsCopy = [];
let i;

for (i = 0; i < len; i++) {
  itemsCopy[i] = items[i];
}

// good
const itemsCopy = [...items];
```

### 4.4 Converting Array-like Objects

-   To convert an array-like object to an array, use Array\#from.

```js
const foo = document.querySelectorAll('.foo');
const nodes = Array.from(foo);
```

Destructuring
-------------

### 5.1 Accessing Multiple Object Properties

-   Use object destructuring when accessing and using multiple
    properties of an object.
-   Be sure to properly document your functions. This is especially
    important when you destructure one or more arguments.

    > Why? Destructuring saves you from creating temporary references
    > for those properties.

```js
// bad
function getFullName(user) {
  const firstName = user.firstName;
  const lastName = user.lastName;

  return `${firstName} ${lastName}`;
}

// good
/**
 * @param  {Object} user
 * @return {String} - The user's full name.
 */
function getFullName(user) {
  const { firstName, lastName } = user;
  return `${firstName} ${lastName}`;
}

// best
/**
 * @param  {Object} user
 * @param  {String} user.firstName
 * @param  {String} user.lastName
 * @return {String} - The user's full name.
 */
function getFullName({ firstName, lastName }) {
  return `${firstName} ${lastName}`;
}
```

#### JSCS Rules

-   [requireObjectDestructuring](http://jscs.info/rule/requireObjectDestructuring)

5.2 Accessing Multiple Array Elements
-------------------------------------

-   Use array destructuring.

```js
const arr = [1, 2, 3, 4];

// bad
const first = arr[0];
const second = arr[1];

// good
const [first, second] = arr;
```

#### JSCS Rules

-   [requireArrayDestructuring](http://jscs.info/rule/requireArrayDestructuring)

### 5.3 Returning Multiple Values

-   Use object destructuring for multiple return values, not array
    destructuring.

    > Why? You can add new properties over time or change the order of
    > things without breaking call sites.

```js
// bad
function processInput(input) {
  // then a miracle occurs
  return [left, right, top, bottom];
}

// the caller needs to think about the order of return data
const [left, __, top] = processInput(input);

// good
function processInput(input) {
  // then a miracle occurs
  return { left, right, top, bottom };
}

// the caller selects only the data they need
const { left, right } = processInput(input);
```

Strings
-------

### 6.1 Quoting Strings

-   Use single quotes `''` for strings.

```js
// bad
const name = "Capt. Janeway";

// good
const name = 'Capt. Janeway';
```

#### ESLint Rules

-   [quotes](http://eslint.org/docs/rules/quotes)

#### JSCS Rules

-   [validateQuoteMarks](http://jscs.info/rule/validateQuoteMarks)

### 6.2 Long Strings

-   Strings that cause the line to go over 100 characters should be
    written across multiple lines using string concatenation.

```js
// bad
const errorMessage = 'This is a super long error that was thrown because of Batman. When you stop to think about how Batman had anything to do with this, you would get nowhere fast.';

// bad
const errorMessage = 'This is a super long error that was thrown because \
of Batman. When you stop to think about how Batman had anything to do \
with this, you would get nowhere \
fast.';

// good
const errorMessage = 'This is a super long error that was thrown because ' +
'of Batman. When you stop to think about how Batman had anything to do ' +
'with this, you would get nowhere fast.';
```

#### A Note on Performance

-   If overused, long strings with concatenation could impact
    performance. [jsPerf](http://jsperf.com/ya-string-concat) &
    [Discussion](https://github.com/airbnb/javascript/issues/40).
-   Runtimes store strings differently based on how they expect the
    strings will be used.

    > The two common types are Ropes and Symbols. Ropes provide memory
    > efficiency for strings created by concatenation or other string
    > manipulations like splitting, but checking equality of Ropes
    > typically costs in proportion to the length of the string. On the
    > other hand, Symbols are unique by their content, so equality
    > checks are a simple pointer comparison.

-   How do I know if my string is a rope or symbol?

    > Typically, static strings created as part of the JS source are
    > stored as Symbols (interned). Strings often used for comparisons
    > can be interned at runtime if some criteria are met. One of these
    > criteria is the size of the entire Rope. For example, in Chrome
    > 38, a Rope longer than 12 characters will not be interned, nor
    > will segments of that Rope.

### 6.3 Building Strings

-   When programmatically building up strings, use template strings
    instead of concatenation.

    > Why? Template strings give you a readable, concise syntax with
    > proper newlines and string interpolation features.

```js
// bad
function sayHi(name) {
  return 'How are you, ' + name + '?';
}

// bad
function sayHi(name) {
  return ['How are you, ', name, '?'].join();
}

// good
function sayHi(name) {
  return `How are you, ${name}?`;
}
```

#### ESLint Rules

-   [prefer-template](http://eslint.org/docs/rules/prefer-template)

#### JSCS Rules

-   [requireTemplateStrings](http://jscs.info/rule/requireTemplateStrings)

### 6.4 Evaluating Strings

-   Never use `eval()` on a string, it opens too many vulnerabilities.

Functions
---------

### 7.1 Naming Functions

-   All functions should be named, including function expressions.
-   Use arrow function syntax to create an anonymous callback when
    performing operations of low complexity.

    > Why? Named functions are easier to identify in call stacks.

```js
// bad
Router.map(function() {
});

// good
Router.map(function setupRoutes() {
});

// good
array.reduce((sum, number) => sum + number, 0);
```

#### ESLint Rules

-   [func-names](http://eslint.org/docs/rules/func-names)
-   [no-restricted-syntax](http://eslint.org/docs/rules/no-restricted-syntax)

### 7.2 Immediately Invoked Function Expressions (IIFE)

-   Wrap each immediately invoked function expression and its invocation
    parens in parens.

    > Why? An immediately invoked function expression is a single unit
    > -wrapping both it, and its invocation parens, in parens, cleanly
    > expresses this. Note that in a world with modules everywhere, you
    > almost never need an IIFE.
    >
    > Need another reason? Google's V8 engine eagerly parses and
    > executes IIFEs when they and their invocation parens are wrapped
    > inside parens. Otherwise, they are lazily parsed, then executed
    > after everything has been parsed.

```js
// immediately-invoked function expression (IIFE)
(function helloWorld() {
  console.log('Welcome to the Internet. Please follow me.');
}());
```

#### ESLint Rules

-   [wrap-iife](http://eslint.org/docs/rules/wrap-iife)

#### JSCS Rules

-   [requireParenthesesAroundIIFE](http://jscs.info/rule/requireParenthesesAroundIIFE)

### 7.3 Declaring Functions

-   Never declare a function in a non-function block (if, while, etc).
    Assign the function to a variable instead. Browsers will allow you
    to do it, but they all interpret it differently, which is bad news
    bears.

```js
// bad
if (currentUser) {
  function test() {
    console.log('Nope.');
  }
}

// good
let test;
if (currentUser) {
  test = () => {
    console.log('Yup.');
  };
}
```

#### ESLint Rules

-   [no-loop-func](http://eslint.org/docs/rules/no-loop-func)

### A Note on Blocks

-   ECMA-262 defines a `block` as a list of statements. A function
    declaration is not a statement. [Read ECMA-262's note on this
    issue](http://www.ecma-international.org/publications/files/ECMA-ST/Ecma-262.pdf#page=97).

### 7.4 Naming Parameters

-   Never name a parameter `arguments`. This will take precedence over
    the `arguments` object that is given to every function scope.

```js
// bad
function nope(name, options, arguments) {
  // ...stuff...
}

// good
function yup(name, options, args) {
  // ...stuff...
}
```

### 7.5 Accepting a Dynamic Number of Arguments

-   Use `arguments` with caution. If arguments must be used, then be
    sure to prefix it with the spread operator `...` when passing it to
    other functions. Never pass `arguments` directly.

    > Why? There are performance issues/deopts related to using `slice`
    > with `arguments`. [Read
    > more](https://github.com/petkaantonov/bluebird/wiki/Optimization-killers#32-leaking-arguments).

```js
// bad (it passes arguments)
function concatenateAll() {
  const args = Array.prototype.slice.call(arguments);
  return args.join('');
}

// good
function concatenateAll(...args) {
  return args.join('');
}

// also good
function concatenateAll() {
  return [...arguments].join('');
}

// also good
concatenateAll() {
  this._super(...arguments);
}
```

### 7.6 Specifying Default Values for Arguments

-   Use default parameter syntax rather than mutating function
    arguments.

```js
// really bad
function handleThings(opts) {
  // No! We shouldn't mutate function arguments.
  // Double bad: if opts is falsy it'll be set to an object which may
  // be what you want but it can introduce subtle bugs.
  opts = opts || {};
  // ...
}

// still bad
function handleThings(opts) {
  if (opts === void 0) {
    opts = {};
  }
  // ...
}

// good
function handleThings(opts = {}) {
  // ...
}
```

#### A Note on Side Effects

-   Never introduce side effects with default parameters.

    > Why? They are confusing to reason about.

```js
var b = 1;
// bad
function count(a = b++) {
  console.log(a);
}
count();  // 1
count();  // 2
count(3); // 3
count();  // 3
```

### 7.7 Ordering Parameters

-   Always put default parameters last.

```js
// bad
function handleThings(opts = {}, name) {
  // ...
}

// good
function handleThings(name, opts = {}) {
  // ...
}
```

### 7.8 Creating Functions

-   Never use the Function constructor to create a new function.

    > Why? Creating a function in this way evaluates a string similarly
    > to eval(), which opens vulnerabilities.

```js
// bad
var add = new Function('a', 'b', 'return a + b');

// still bad
var subtract = Function('a', 'b', 'return a - b');
```

#### ESLint Rules

-   [no-new-func](http://eslint.org/docs/rules/no-new-func)

### 7.9 Spacing in a Function Signature

-   Place a single space between the parens and braces in a function
    signature.

    > Why? Consistency is good, and all functions should be named (per
    > [7.1](#naming-functions)).

```js
// bad
const f = function(){};
const g = function() {};
const h = function (){};
const x = function () {};
const y = function a(){};

// good
const z = function a() {};
```

### 7.10 Mutating Parameters

-   Never mutate parameters.

    > Why? Manipulating objects passed in as parameters can cause
    > unwanted variable side effects in the original caller.
    >
    > Why not? If you're integrating an external API, there may be no
    > alternative to mutating a parameter.

```js
// bad
function f1(obj) {
  obj.key = someCondition() ? obj.key : 1;
};

// good
function f2(obj) {
  const value = someCondition() ? obj.key : 1;
};
```

#### ESLint Rules

-   [no-param-reassign](http://eslint.org/docs/rules/no-param-reassign)

### 7.11 Reassigning Parameters

-   Never reassign parameters.

    > Why? Reassigning parameters can lead to unexpected behavior,
    > especially when accessing the `arguments` object. It can also
    > cause optimization issues, especially in V8.

```js
// bad
function f1(a) {
  a = 1;
}

function f2(a) {
  if (!a) { a = 1; }
}

// good
function f3(a) {
  const b = a || 1;
}

function f4(a = 1) {
}
```

#### ESLint Rules

-   [no-param-reassign](http://eslint.org/docs/rules/no-param-reassign)

Arrow Functions
---------------
### 8.1 Using Arrow Functions

-   When you must use an anonymous function, use arrow function
    notation.

    > Why? It creates a version of the function that executes in the
    > context of `this`, which is usually what you want, and is a more
    > concise syntax.
    >
    > Why not? If you have a fairly complicated function, you might move
    > that logic out into its own function declaration.

```js
// bad
[1, 2, 3].map(function (x) {
  const y = x + 1;
  return x * y;
});

// good
[1, 2, 3].map((x) => {
  const y = x + 1;
  return x * y;
});
```

#### ESLint Rules

-   [prefer-arrow-callback](http://eslint.org/docs/rules/prefer-arrow-callback)
-   [arrow-spacing](http://eslint.org/docs/rules/arrow-spacing)

#### JSCS Rules

-   [requireArrowFunctions](http://jscs.info/rule/requireArrowFunctions)

### 8.2 Arrow Function Braces

-   If the function body consists of a single expression, omit the
    braces and use the implicit return. Otherwise, keep the braces and
    use a `return` statement.

    > Why? Syntactic sugar. It reads well when multiple functions are
    > chained together.
    >
    > If you plan on returning an object, wrap it in parens.

```js
// good
[1, 2, 3].map(number => `A string containing the ${number}.`);

// bad
[1, 2, 3].map(number => {
  const nextNumber = number + 1;
  `A string containing the ${nextNumber}.`;
});

// good
[1, 2, 3].map((number) => {
  const nextNumber = number + 1;
  return `A string containing the ${nextNumber}.`;
});

// good
[1, 2, 3].map(number => ({
  value: number
}));
```

#### ESLint Rules

-   [arrow-parens](http://eslint.org/docs/rules/arrow-parens)
-   [arrow-body-style](http://eslint.org/docs/rules/arrow-body-style)

#### JSCS Rules

-   [disallowParenthesesAroundArrowParam](http://jscs.info/rule/disallowParenthesesAroundArrowParam)
-   [requireShorthandArrowFunctions](http://jscs.info/rule/requireShorthandArrowFunctions)

### 8.3 Multiline Expressions

-   In case the expression spans over multiple lines, wrap it in
    parentheses for better readability.

    > Why? It shows clearly where the function starts and ends.

```js
// bad
[1, 2, 3].map(number => 'As time went by, the string containing the ' +
`${number} became much longer. So we needed to break it over multiple ` +
'lines.'
);

// good
[1, 2, 3].map(number => (
  `As time went by, the string containing the ${number} became much ` +
  'longer. So we needed to break it over multiple lines.'
));
```

### 8.4 Arrow Function Argument Parentheses

-   If your function takes a single argument and doesnâ€™t use braces,
    omit the parentheses. Otherwise, always include parentheses around
    arguments.

    > Why? Less visual clutter.

```js
// bad
[1, 2, 3].map((x) => x * x);

// good
[1, 2, 3].map(x => x * x);

// good
[1, 2, 3].map(number => (
`A long string with the ${number}. Itâ€™s so long that weâ€™ve broken it ` +
'over multiple lines!'
));

// bad
[1, 2, 3].map(x => {
  const y = x + 1;
  return x * y;
});

// good
[1, 2, 3].map((x) => {
  const y = x + 1;
  return x * y;
});
```

-   If your function takes no arguments, use empty parentheses.

    > Why? It is descriptive of your intent and results in fewer
    > characters after ES5 transpilation.

```js
// bad
foo('hey', _ => alert('done'));

// good
foo('hey', () => alert('done'));
```

#### ESLint Rules

-   [arrow-parens](http://eslint.org/docs/rules/arrow-parens)

    > We will need to write a custom rule (or extend the current rule,
    > if possible) so we can require parens for arrow functions with
    > block statements.

#### JSCS Rules

-   [disallowParenthesesAroundArrowParam](http://jscs.info/rule/disallowParenthesesAroundArrowParam)

Constructors
------------

### 9.1 Creating Classes

-   Use `class`. Avoid manipulating `prototype` directly.

    > Why? `class` syntax is more concise and easier to reason about.

```js
// bad
function Queue(contents = []) {
  this._queue = [...contents];
}
Queue.prototype.pop = function () {
  const value = this._queue[0];
  this._queue.splice(0, 1);
  return value;
}

// good
class Queue {
  constructor(contents = []) {
    this._queue = [...contents];
  }
  pop() {
    const value = this._queue[0];
    this._queue.splice(0, 1);
    return value;
  }
}
```

### 9.2 Inheritance

-   Use `extends` for inheritance.

    > Why? It is a built-in way to inherit prototype functionality
    > without breaking `instanceof`.

```js
// bad
const inherits = require('inherits');
function PeekableQueue(contents) {
  Queue.apply(this, contents);
}
inherits(PeekableQueue, Queue);
PeekableQueue.prototype.peek = function () {
  return this._queue[0];
}

// good
class PeekableQueue extends Queue {
  peek() {
    return this._queue[0];
  }
}
```

### 9.3 `toString()`

-   It's okay to write a custom toString() method, just make sure it
    works successfully and causes no side effects.

```js
class Jedi {
  constructor(options = {}) {
    this.name = options.name || 'no name';
  }

  getName() {
    return this.name;
  }

  toString() {
    return `Jedi - ${this.getName()}`;
  }
}
```

Modules
-------
### 10.1 Importing and Exporting

-   Always use modules (`import`/`export`) over a non-standard module
    system. You can always transpile to your preferred module system.

    > Why? Modules are the future. Let's start using the future now.

```js
// bad
const AirbnbStyleGuide = require('./airbnb-style-guide');
module.exports = AirbnbStyleGuide.es6;

// ok
import AirbnbStyleGuide from './airbnb-style-guide';
export default AirbnbStyleGuide.es6;

// best
import { es6 } from './airbnb-style-guide';
export default es6;
```

### 10.2 Exporting an Import

-   Exporting directly from an import is encouraged.

    > Why? The one-liner is concise, and the Ember community has
    > espoused this approach.

```js
// good
// filename es6.js
export { es6 as default } from './airbnb-style-guide';
```

Iterators and Generators
------------------------

### 11.1 Iterators

-   Don't use iterators. Prefer JavaScript's higher-order functions like
    `map()` and `reduce()` instead of loops like `for-of`.

    > Why? This enforces our immutable rule. Dealing with pure functions
    > that return values is easier to reason about than side-effects.

```js
const numbers = [1, 2, 3, 4, 5];

// bad
let sum = 0;
for (let num of numbers) {
  sum += num;
}

sum === 15;

// bad
let sum = 0;
numbers.forEach(num => sum += num);
sum === 15;

// best (use the functional force)
const sum = numbers.reduce((total, num) => total + num, 0);
sum === 15;
```

#### ESLint Rules

-   [no-iterator](http://eslint.org/docs/rules/no-iterator)

### 11.2 Generators

-   Don't use generators for now.

    > Why? They don't transpile well to ES5.

Properties
----------

### 12.1 Accessing Properties

-   Use dot notation when accessing properties.

```js
const luke = {
  jedi: true,
  age: 28,
};

// bad
const isJedi = luke['jedi'];

// good
const isJedi = luke.jedi;
```

#### ESLint Rules

-   [dot-notation](http://eslint.org/docs/rules/dot-notation)

#### JSCS Rules

-   [requireDotNotation](http://jscs.info/rule/requireDotNotation)

### 12.2 Accessing Properties with a Variable

-   Use subscript notation `[]` when accessing properties with a
    variable.

```js
const luke = {
  jedi: true,
  age: 28,
};

function getProp(prop) {
  return luke[prop];
}

const isJedi = getProp('jedi');
```

Variables
---------
### 13.1 Declaring Variables

-   Always use `const` to declare variables. Not doing so will result in
    global variables. We want to avoid polluting the global namespace.
    Captain Planet warned us of that.

```js
// bad
superPower = new SuperPower();

// good
const superPower = new SuperPower();
```

### 13.2 Multiple Declarations

-   Use one `const`/`let` declaration per variable.

    > Why? It's easier to add new variable declarations this way, and
    > you never have to worry about swapping out a `;` for a `,` or
    > introducing punctuation-only diffs.

```js
// bad
const items = getItems(),
    goSportsTeam = true,
    dragonball = 'z';

// bad
// (compare to above, and try to spot the mistake)
const items = getItems(),
    goSportsTeam = true;
    dragonball = 'z';

// good
const items = getItems();
const goSportsTeam = true;
const dragonball = 'z';
```

#### ESLint Rules

-   [one-var](http://eslint.org/docs/rules/one-var)

#### JSCS Rules

-   [disallowMultipleVarDecl](http://jscs.info/rule/disallowMultipleVarDecl)

### 13.3 Grouping Declarations

-   Group all your `const`s and then group all your `let`s.

    > Why? This is helpful when later on you might need to assign a
    > variable depending on one of the previous assigned variables.

```js
// bad
let i, len, dragonball,
    items = getItems(),
    goSportsTeam = true;

// bad
let i;
const items = getItems();
let dragonball;
const goSportsTeam = true;
let len;

// good
const goSportsTeam = true;
const items = getItems();
let dragonball;
let i;
let length;
```

### 13.4 Assigning Variables

-   Assign variables where you need them, but place them in a reasonable
    place.

    > Why? `let` and `const` are block scoped and not function scoped.

```js
// bad - unnecessary function call
function checkName(hasName) {
  const name = getName();

  if (hasName === 'test') {
    return false;
  }

  if (name === 'test') {
    this.setName('');
    return false;
  }

  return name;
}

// good
function checkName(hasName) {
  if (hasName === 'test') {
    return false;
  }

  const name = getName();

  if (name === 'test') {
    this.setName('');
    return false;
  }

  return name;
}
```

Notes on Hoisting
-----------------
### 14.1 Variable Hoisting

-   `var` declarations get hoisted to the top of their scope, their
    assignment does not. `const` and `let` declarations are blessed with
    a new concept called [Temporal Dead Zones
    (TDZ)](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/let#Temporal_dead_zone_and_errors_with_let).
    It's important to know how [this affects typeof
    usage](http://es-discourse.com/t/why-typeof-is-no-longer-safe/15).

```js
// we know this wouldn't work (assuming there
// is no notDefined global variable)
function example() {
  console.log(notDefined); // => throws a ReferenceError
}

// creating a variable declaration after you
// reference the variable will work due to
// variable hoisting. Note: the assignment
// value of `true` is not hoisted.
function example() {
  console.log(declaredButNotAssigned); // => undefined
  var declaredButNotAssigned = true;
}

// the interpreter is hoisting the variable
// declaration to the top of the scope,
// which means our example could be rewritten as:
function example() {
  let declaredButNotAssigned;
  console.log(declaredButNotAssigned); // => undefined
  declaredButNotAssigned = true;
}

// using const and let
function example() {
  console.log(declaredButNotAssigned); // => throws a ReferenceError
  console.log(typeof declaredButNotAssigned); // => throws a ReferenceError
  const declaredButNotAssigned = true;
}
```

### 14.2 Anonymous Function Expression Hoisting

-   Anonymous function expressions hoist their variable name, but not
    the function assignment. Per [7.1](#naming-functions), you shouldn't
    be using anonymous function expressions anyway.

```js
function example() {
  console.log(anonymous); // => undefined

  anonymous(); // => TypeError anonymous is not a function

  var anonymous = function () {
    console.log('anonymous function expression');
  };
}
```

### 14.3 Named Function Expression Hoisting

-   Named function expressions hoist the variable name, not the function
    name or the function body.

```js
function example() {
  console.log(named); // => undefined

  named(); // => TypeError named is not a function

  superPower(); // => ReferenceError superPower is not defined

  var named = function superPower() {
    console.log('Flying');
  };
}

// the same is true when the function name
// is the same as the variable name.
function example() {
  console.log(named); // => undefined

  named(); // => TypeError named is not a function

  var named = function named() {
    console.log('named');
  }
}
```

### 14.4 Function Declaration Hoisting

-   Function declarations hoist their name and the function body.

```js
function example() {
  superPower(); // => Flying

  function superPower() {
    console.log('Flying');
  }
}
```

### 14.5 More Information

-   For more information refer to [JavaScript Scoping &
    Hoisting](http://www.adequatelygood.com/2010/2/JavaScript-Scoping-and-Hoisting/)
    by [Ben Cherry](http://www.adequatelygood.com/).

Comparison Operators & Equality
-------------------------------
### 15.1 A Note on Boolean Coercion

-   Conditional statements, such as the `if` statement, evaluate their
    expression using coercion with the `ToBoolean` abstract method and
    always follow these simple rules:
-   **Objects** evaluate to **true**
-   **Undefined** evaluates to **false**
-   **Null** evaluates to **false**
-   **Booleans** evaluate to **the value of the boolean**
-   **Numbers** evaluate to **false** if **+0, -0, or NaN**, otherwise
    **true**
-   **Strings** evaluate to **false** if an empty string `''`, otherwise
    **true**

```js
if ([0] && []) {
  // true
  // an array (even an empty one) is an object, objects will evaluate to true
}
```

### More Information

-   For more information see [Truth Equality and
    JavaScript](http://javascriptweblog.wordpress.com/2011/02/07/truth-equality-and-javascript/#more-2108)
    by Angus Croll.

### 15.2 Testing Equality

-   Use `===` and `!==` over `==` and `!=` with one exception: use `==`
    to test for undefined/null.

#### ESLint Rules

-   [eqeqeq](http://eslint.org/docs/rules/eqeqeq)
-   [no-eq-null](http://eslint.org/docs/rules/no-eq-null)

### 15.3 Shortcuts

-   Use shortcuts.

```js
// bad
if (name !== '') {
  // ...stuff...
}

// good
if (name) {
  // ...stuff...
}

// bad
if (collection.length > 0) {
  // ...stuff...
}

// good
if (collection.length) {
  // ...stuff...
}
```

### 15.4 Blocks in `case` Clauses

-   Use braces to create blocks in `case` and `default` clauses that
    contain lexical declarations (e.g. `let`, `const`, `function`, and
    `class`).

    > Why? Lexical declarations are visible in the entire `switch` block
    > but only get initialized when assigned, which only happens when
    > its `case` is reached. This causes problems when multiple `case`
    > clauses attempt to define the same thing.

```js
// bad
switch (foo) {
  case 1:
    let x = 1;
    break;
  case 2:
    const y = 2;
    break;
  case 3:
    function f() {}
    break;
  default:
    class C {}
}

// good
switch (foo) {
  case 1: {
    let x = 1;
    break;
  }
  case 2: {
    const y = 2;
    break;
  }
  case 3: {
    function f() {}
    break;
  }
  case 4:
    bar();
    break;
  default: {
    class C {}
  }
}
```

#### ESLint Rules

-   [no-case-declarations](http://eslint.org/docs/rules/no-case-declarations)

### 15.4 Ternaries

-   Ternaries should not be nested and should generally be single line
    expressions.

```js
// bad
const foo = maybe1 > maybe2
  ? "bar"
  : value1 > value2 ? "baz" : null;

// better
const maybeNull = value1 > value2 ? 'baz' : null;

const foo = maybe1 > maybe2
  ? 'bar'
  : maybeNull;

// best
const maybeNull = value1 > value2 ? 'baz' : null;

const foo = maybe1 > maybe2 ? 'bar' : maybeNull;
```

-   Avoid unneeded ternary statements.

```js
// bad
const foo = a ? a : b;
const bar = c ? true : false;
const baz = c ? false : true;

// good
const foo = a || b;
const bar = !!c;
const baz = !c;
```

#### ESLint Rules

-   [no-nested-ternary](http://eslint.org/docs/rules/no-nested-ternary)
-   [no-unneeded-ternary](http://eslint.org/docs/rules/no-unneeded-ternary)

Blocks
------

### 16.1 Multiline Blocks

-   Use braces with all multi-line blocks, and multi-line blocks with
    all control statements.

```js
// bad
if (test)
  return false;

// bad
if (test) return false;

// good
if (test) {
  return false;
}

// bad
function foo() { return false; }

// good
function bar() {
  return false;
}
```

#### ESLint Rules

-   [curly](http://eslint.org/docs/rules/curly)

### 16.2 Else Placement

-   If you're using multi-line blocks with `if` and `else`, put `else`
    on the same line as your `if` block's closing brace.

```js
// bad
if (test) {
  thing1();
  thing2();
}
else {
  thing3();
}

// good
if (test) {
  thing1();
  thing2();
} else {
  thing3();
}
```

#### ESLint Rules

-   [brace-style](http://eslint.org/docs/rules/brace-style)

#### JSCS Rules

-   [disallowNewlineBeforeBlockStatements](http://jscs.info/rule/disallowNewlineBeforeBlockStatements)

Comments
--------

### 17.1 Doc Comments

-   Use `/** ... */` for multi-line comments. Include a description,
    specify types and values for all parameters and return values.

```js
// bad
// make() returns a new element
// based on the passed in tag name
//
// @param {String} tag
// @return {Element} element
function make(tag) {

  // ...stuff...

  return element;
}

// good
/**
 * make() returns a new element
 * based on the passed in tag name
 *
 * @param {String} tag
 * @return {Element} element
 */
function make(tag) {

  // ...stuff...

  return element;
}
```

### 17.2 Inline Comments

-   Use `//` for single line comments. Place single line comments on a
    newline above the subject of the comment. Put an empty line before
    the comment unless it's on the first line of a block.

```js
// bad
const active = true;  // is current tab

// good
// is current tab
const active = true;

// bad
function getType() {
  console.log('fetching type...');
  // set the default type to 'no type'
  const type = this._type || 'no type';

  return type;
}

// good
function getType() {
  console.log('fetching type...');

  // set the default type to 'no type'
  const type = this._type || 'no type';

  return type;
}

// also good
function getType() {
  // set the default type to 'no type'
  const type = this._type || 'no type';

  return type;
}
```

### 17.3 FIXME and TODO

-   Prefixing your comments with `FIXME` or `TODO` helps other
    developers quickly understand if you're pointing out a problem that
    needs to be revisited, or if you're suggesting a solution to the
    problem that needs to be implemented. These are different than
    regular comments because they are actionable. The actions are
    `FIXME -- need to figure this out` or `TODO -- need to implement`.

### 17.4 Using FIXME

-   Use `// FIXME:` to annotate problems.

```js
class Calculator extends Abacus {
  constructor() {
    super();

    // FIXME: shouldn't use a global here
    total = 0;
  }
}
```

### 17.5 Using TODO

-   Use `// TODO:` to annotate solutions to problems.

```js
class Calculator extends Abacus {
  constructor() {
    super();

    // TODO: total should be configurable by an options param
    this.total = 0;
  }
}
```

Whitespace
----------

### 18.1 Tabbing

-   Use soft tabs set to 2 spaces.

```js
// bad
function foo() {
âˆ™âˆ™âˆ™âˆ™const name;
}

// bad
function bar() {
âˆ™const name;
}

// good
function baz() {
âˆ™âˆ™const name;
}
```

#### ESLint Rules

-   [indent](http://eslint.org/docs/rules/indent)

#### JSCS Rules

-   [validateIndentation](http://jscs.info/rule/validateIndentation)

### 18.2 Spaces Before Blocks

-   Place 1 space before the leading brace.

```js
// bad
function test(){
  console.log('test');
}

// good
function test() {
  console.log('test');
}

// bad
dog.set('attr',{
  age: '1 year',
  breed: 'Bernese Mountain Dog',
});

// good
dog.set('attr', {
  age: '1 year',
  breed: 'Bernese Mountain Dog',
});
```

#### ESLint Rules

-   [space-before-blocks](http://eslint.org/docs/rules/space-before-blocks)

#### JSCS Rules

-   [requireSpaceBeforeBlockStatements](http://jscs.info/rule/requireSpaceBeforeBlockStatements)

### 18.3 Spaces Before Parentheses

-   Place 1 space before the opening parenthesis in control statements
    (`if`, `while` etc.).
-   Place no space between the argument list and the function name in
    function calls and declarations.

```js
// bad
if(isJedi) {
  fight ();
}

// good
if (isJedi) {
  fight();
}

// bad
function fight () {
  console.log ('Swooosh!');
}

// good
function fight() {
  console.log('Swooosh!');
}
```

#### ESLint Rules

-   [space-after-keywords](http://eslint.org/docs/rules/space-after-keywords)
-   [space-before-keywords](http://eslint.org/docs/rules/space-before-keywords)

#### JSCS Rules

-   [requireSpaceAfterKeywords](http://jscs.info/rule/requireSpaceAfterKeywords)

### 18.4 Spaces Around Operators

-   Set off operators with spaces.

```js
// bad
const x=y+5;

// good
const x = y + 5;
```

#### ESLint Rules

-   [space-infix-ops](http://eslint.org/docs/rules/space-infix-ops)

#### JSCS Rules

-   [requireSpaceBeforeBinaryOperators](http://jscs.info/rule/requireSpaceBeforeBinaryOperators)
-   [requireSpaceAfterBinaryOperators](http://jscs.info/rule/requireSpaceAfterBinaryOperators)

### 18.5 End of Files

-   End files with a single newline character.

```js
// bad
(function (global) {
  // ...stuff...
})(this);
```

```js
// bad
(function (global) {
  // ...stuff...
})(this);â†µ
â†µ
```

```js
// good
(function (global) {
  // ...stuff...
})(this);â†µ
```

### 18.6 Method Chaining

-   Use indentation when making long method chains.
-   Use a leading dot, which emphasizes that the line is a method call,
    not a new statement.

```js
// bad
$('#items').find('.selected').highlight().end().find('.open').updateCount();

// bad
$('#items').
  find('.selected').
    highlight().
    end().
  find('.open').
    updateCount();

// good
$('#items')
  .find('.selected')
    .highlight()
    .end()
  .find('.open')
    .updateCount();

// bad
const leds = stage.selectAll('.led').data(data).enter().append('svg:svg').class('led', true)
    .attr('width', (radius + margin) * 2).append('svg:g')
    .attr('transform', 'translate(' + (radius + margin) + ',' + (radius + margin) + ')')
    .call(tron.led);

// good
const leds = stage.selectAll('.led')
    .data(data)
  .enter().append('svg:svg')
    .classed('led', true)
    .attr('width', (radius + margin) * 2)
  .append('svg:g')
    .attr('transform', 'translate(' + (radius + margin) + ',' + (radius + margin) + ')')
    .call(tron.led);
```

### 18.7 Space After Blocks

-   Leave a blank line after blocks and before the next statement.

```js
// bad
if (foo) {
  return bar;
}
return baz;

// good
if (foo) {
  return bar;
}

return baz;

// bad
const obj = {
  foo() {
  },
  bar() {
  },
};
return obj;

// good
const obj = {
  foo() {
  },

  bar() {
  },
};

return obj;

// bad
const arr = [
  function foo() {
  },
  function bar() {
  },
];
return arr;

// good
const arr = [
  function foo() {
  },

  function bar() {
  },
];

return arr;
```

#### JSCS Rules

-   [requirePaddingNewLinesAfterBlocks](http://jscs.info/rule/requirePaddingNewLinesAfterBlocks)

### 18.8 Block Padding

-   Do not pad your blocks with blank lines.

```js
// bad
function bar() {

  console.log(foo);

}

// also bad
if (baz) {

  console.log(qux);
} else {
  console.log(foo);

}

// good
function bar() {
  console.log(foo);
}

// good
if (baz) {
  console.log(qux);
} else {
  console.log(foo);
}
```

#### ESLint Rules

-   [padded-blocks](http://eslint.org/docs/rules/padded-blocks)

#### JSCS Rules

-   [disallowPaddingNewlinesInBlocks](http://jscs.info/rule/disallowPaddingNewlinesInBlocks)

### 18.9 Spaces Inside Parentheses

-   Do not add spaces inside parentheses.

```js
// bad
function bar( foo ) {
  return foo;
}

// good
function bar(foo) {
  return foo;
}

// bad
if ( foo ) {
  console.log(foo);
}

// good
if (foo) {
  console.log(foo);
}
```

#### ESLint Rules

-   [space-in-parens](http://eslint.org/docs/rules/space-in-parens)

#### JSCS Rules

-   [disallowSpacesInsideParentheses](http://jscs.info/rule/disallowSpacesInsideParentheses)

### 18.10 Spaces Inside Brackets

-   Do not add spaces inside brackets.

```js
// bad
const foo = [ 1, 2, 3 ];
console.log(foo[ 0 ]);

// good
const foo = [1, 2, 3];
console.log(foo[0]);
```

#### ESLint Rules

-   [array-bracket-spacing](http://eslint.org/docs/rules/array-bracket-spacing)

#### JSCS Rules

-   [disallowSpacesInsideArrayBrackets](http://jscs.info/rule/disallowSpacesInsideArrayBrackets)

### 18.11 Spaces Inside Curly Braces

-   Add spaces inside curly braces.

```js
// bad
const {name: identity} = clarkKent;

// good
const { name: identity } = clarkKent;
```

#### ESLint Rules

-   [object-curly-spacing](http://eslint.org/docs/rules/object-curly-spacing)

#### JSCS Rules

-   [disallowSpacesInsideObjectBrackets](http://jscs.info/rule/disallowSpacesInsideObjectBrackets)

Commas
------

### 19.1 Leading Commas

-   **Nope.**

```js
// bad
const story = [
    once
  , upon
  , aTime
];

// good
const story = [
  once,
  upon,
  aTime,
];

// bad
const hero = {
    firstName: 'Ada'
  , lastName: 'Lovelace'
  , birthYear: 1815
  , superPower: 'computers'
};

// good
const hero = {
  firstName: 'Ada',
  lastName: 'Lovelace',
  birthYear: 1815,
  superPower: 'computers',
};
```

#### ESLint Rules

-   [comma-style](http://eslint.org/docs/rules/comma-style)

#### JSCS Rules

-   [requireCommaBeforeLineBreak](http://jscs.info/rule/requireCommaBeforeLineBreak)

### 19.2 Trailing Comma

-   **Yup.**

    > Why? This leads to cleaner git diffs and makes it easier to
    > reorder properties in object literals and elements in arrays.
    > Also, transpilers like Babel will remove the additional trailing
    > comma in the transpiled code which means you don't have to worry
    > about the [trailing comma problem](es5/README.md#commas) in legacy
    > browsers.

```js
// bad - git diff without trailing comma
const hero = {
     firstName: 'Florence',
  -    lastName: 'Nightingale'
  +    lastName: 'Nightingale',
  +    inventorOf: ['coxcomb graph', 'modern nursing']
};

// good - git diff with trailing comma
const hero = {
     firstName: 'Florence',
     lastName: 'Nightingale',
  +    inventorOf: ['coxcomb chart', 'modern nursing'],
};

// bad
const hero = {
  firstName: 'Dana',
  lastName: 'Scully'
};

const heroes = [
  'Batman',
  'Superman'
];

// good
const hero = {
  firstName: 'Dana',
  lastName: 'Scully',
};

const heroes = [
  'Batman',
  'Superman',
];
```

#### ESLint Rules

-   [comma-dangle](http://eslint.org/docs/rules/comma-dangle)

#### JSCS Rules

-   [requireTrailingComma](http://jscs.info/rule/requireTrailingComma)

Semicolons
----------

### 20.1 Semicolons

-   **Yup.**

```js
// bad
(function () {
  const name = 'Skywalker'
  return name
})()

// good
(() => {
  const name = 'Skywalker';
  return name;
}());

// good (guards against the function becoming an argument when two files with IIFEs are concatenated)
;(() => {
  const name = 'Skywalker';
  return name;
}());
```

[Read
more](http://stackoverflow.com/questions/7365172/semicolon-before-self-invoking-function/7365214%237365214)
about leading semicolons and IIFEs.

#### ESLint Rules

-   [semi](http://eslint.org/docs/rules/semi)

#### JSCS Rules

-   [requireSemicolons](http://jscs.info/rule/requireSemicolons)

Type Casting & Coercion
-----------------------
### 21.1 Performing Coercion

-   Perform type coercion at the beginning of the statement.

    > For a deep dive on JavaScript coercion, read [You Don't Know JS:
    > Types & Grammar, Chapter 4:
    > Coercion](https://github.com/getify/You-Dont-Know-JS/blob/master/types%20%26%20grammar/ch4.md#chapter-4-coercion)

### 21.2 Strings

-   Use ES6 template literals.

    > Why? It uses the same number of characters as empty-string
    > concatenation and serves to bolster consistency.

```js
// => this.reviewScore = 9;

// bad
const totalScore = this.reviewScore + '';

// bad
const totalScore = '' + this.reviewScore;

// okay
const totalScore = String(this.reviewScore);

// best
const totalScore = `${this.reviewScore}`;
```

#### ESLint Rules

-   [no-implicit-coercion](http://eslint.org/docs/rules/no-implicit-coercion)
-   [prefer-template](http://eslint.org/docs/rules/prefer-template)

### 21.3 Numbers

-   Use `(+foo)`, always with parens, for type casting and `parseInt`,
    always with a radix, for parsing strings.

    > Why parens? It prevents confusing situations like `bar=+foo` vs
    > `bar+=foo` and `+foo` vs `++foo`.

```js
const inputValue = '4';

// bad
const val = new Number(inputValue);

// bad
const val = inputValue >> 0;

// bad
const val = parseInt(inputValue);

// bad
const val = +inputValue;

// okay
const val = Number(inputValue);

// best
const val = (+inputValue);

// best
const val = parseInt(inputValue, 10);
```

#### ESLint Rules

-   [no-implicit-coercion](http://eslint.org/docs/rules/no-implicit-coercion)
-   [radix](http://eslint.org/docs/rules/radix)

### 21.4 Bit Shifting

-   If for whatever reason you are doing something wild and `parseInt`
    is your bottleneck, and you need to use a bit shift for [performance
    reasons](http://jsperf.com/coercion-vs-casting/3), leave a comment
    explaining why and what you're doing.

```js
// good
/**
 * parseInt was the reason my code was slow.
 * Bit shifting the String to coerce it to a
 * Number made it a lot faster.
 */
const val = inputValue >> 0;
```

### A Note on Bit Shifting

-   Be careful when using bit shift operations. Numbers are represented
    as [64-bit values](http://es5.github.io/#x4.3.19), but bit shift
    operations always return a 32-bit integer
    ([source](http://es5.github.io/#x11.7)). Bit shifting can lead to
    unexpected behavior for integer values larger than 32 bits.
    [Discussion](https://github.com/airbnb/javascript/issues/109).
    Largest signed 32-bit Int is 2,147,483,647:

```js
2147483647 >> 0 //=> 2147483647
2147483648 >> 0 //=> -2147483648
2147483649 >> 0 //=> -2147483647
```

### 21.6 Booleans

```js
const age = 0;

// bad
const hasAge = new Boolean(age);

// okay
const hasAge = Boolean(age);

// best
const hasAge = !!age;
```

#### ESLint Rules

-   [no-implicit-coercion](http://eslint.org/docs/rules/no-implicit-coercion)

Naming Conventions
------------------

### 22.1 Descriptiveness

-   Avoid single letter names. Be descriptive with your naming.

```js
// bad
function q() {
  // ...stuff...
}

// good
function query() {
  // ..stuff..
}
```

### 22.2 camelCase

-   Use camelCase when naming objects, functions, and instances.

```js
// bad
const OBJEcttsssss = {};
const this_is_my_object = {};
function c() {}

// good
const thisIsMyObject = {};
function thisIsMyFunction() {}
```

#### ESLint Rules

-   [camelcase](http://eslint.org/docs/rules/camelcase)

#### JSCS Rules

-   [requireCamelCaseOrUpperCaseIdentifiers](http://jscs.info/rule/requireCamelCaseOrUpperCaseIdentifiers)

### 22.3 PascalCase

-   Use PascalCase when naming constructors or classes.

```js
// bad
function user(options) {
  this.name = options.name;
}

const bad = new user({
  name: 'nope',
});

// good
class User {
  constructor(options) {
    this.name = options.name;
  }
}

const good = new User({
  name: 'yup',
});
```

#### ESLint Rules

-   [new-cap](http://eslint.org/docs/rules/new-cap)

#### JSCS Rules

-   [requireCapitalizedConstructors](http://jscs.info/rule/requireCapitalizedConstructors)

### 22.4 CAPS\_CASE

-   Use CAPS\_CASE when naming constants.

```js
// bad
const badConstant = 'Some error string or something.';

// good
const GOOD_CONSTANT = 'Some error string or something.';
```

### 22.5 Private Properties

-   Use a leading underscore `_` when naming private properties.

```js
// bad
this.__firstName__ = 'Panda';
this.firstName_ = 'Panda';

// good
this._firstName = 'Panda';
```

#### ESLint Rules

-   [no-underscore-dangle](http://eslint.org/docs/rules/no-underscore-dangle)

#### JSCS Rules

-   [disallowDanglingUnderscores](http://jscs.info/rule/disallowDanglingUnderscores)

### 22.6 Referencing `this`

-   Don't save references to `this`.
-   Use arrow functions or Function\#bind.

```js
// bad
function foo() {
  const self = this;
  return function () {
    console.log(self);
  };
}

// bad
function foo() {
  const that = this;
  return function () {
    console.log(that);
  };
}

// good
function foo() {
  return () => {
    console.log(this);
  };
}
```

#### JSCS Rules

-   [disallowNodeTypes](http://jscs.info/rule/disallowNodeTypes)

### 22.7 Exporting Classes

-   If your file exports a single class, its filename should be exactly
    the name of the class, but dasherized.

```js
// file contents
class CheckBox {
  // ...
}
export default CheckBox;

// in some other file
// bad
import CheckBox from './checkBox';

// bad
import CheckBox from './CheckBox';

// bad
import CheckBox from './check_box';

// good
import CheckBox from './check-box';
```

### 22.8 Exporting Functions

-   Use camelCase when you export-default a function. The filename
    should be identical to your function's name, but dasherized.

```js
function makeStyleGuide() {
}

export default makeStyleGuide;
```

### 22.9 Exporting Singletons, Libraries, and Objects

-   Use PascalCase when you export a singleton, function library, or
    bare object.

```js
const AirbnbStyleGuide = {
  es6: {
  }
};

export default AirbnbStyleGuide;
```

Accessors
---------
### 23.1 Accessor Functions

-   Accessor functions for properties are not required.

### 23.2 Getters and Setters

-   If you do make accessor functions, use `getVal()` and
    `setVal('hello')`.

```js
// bad
dragon.age();

// good
dragon.getAge();

// bad
dragon.age(25);

// good
dragon.setAge(25);
```

### 23.3 Boolean Getters

-   If the property is a Boolean, use `isVal()` or `hasVal()`.

```js
// bad
if (!dragon.age()) {
  return false;
}

// good
if (!dragon.hasAge()) {
  return false;
}
```

### 23.4 `get()` and `set()`

-   It's okay to create get() and set() functions, but be consistent.

```js
class Jedi {
  constructor(options = {}) {
    const lightsaber = options.lightsaber || 'blue';
    this.set('lightsaber', lightsaber);
  }

  set(key, val) {
    this[key] = val;
  }

  get(key) {
    return this[key];
  }
}
```

Events
------

### 24.1 Data Payloads

-   When attaching data payloads to events (whether DOM events or
    something more proprietary like Backbone events), pass a hash
    instead of a raw value. This allows a subsequent contributor to add
    more data to the event payload without finding and updating every
    handler for the event.

For example, instead of:

```js
// bad
$(this).trigger('listingUpdated', listing.id);

...

$(this).on('listingUpdated', (e, listingId) => {
  // do something with listingId
});
```

prefer:

```js
// good
$(this).trigger('listingUpdated', { listingId: listing.id });

...

$(this).on('listingUpdated', (e, data) => {
  // do something with data.listingId
});
```

jQuery
------

### 25.1 jQuery Object Variables

-   Prefix jQuery object variables with a `$`.

```js
// bad
const sidebar = $('.sidebar');

// good
const $sidebar = $('.sidebar');

// good
const $sidebarBtn = $('.sidebar-btn');
```

#### JSCS Rules

-   [requireDollarBeforejQueryAssignment](http://jscs.info/rule/requireDollarBeforejQueryAssignment)

### 25.2 jQuery Lookups

-   Cache jQuery lookups.

```js
// bad
function setSidebar() {
  $('.sidebar').hide();

  // ...stuff...

  $('.sidebar').css({
    'background-color': 'pink'
  });
}

// good
function setSidebar() {
  const $sidebar = $('.sidebar');
  $sidebar.hide();

  // ...stuff...

  $sidebar.css({
    'background-color': 'pink'
  });
}
```

### 25.3 DOM Queries

-   For DOM queries, use Cascading `$('.sidebar ul')` or parent &gt;
    child `$('.sidebar > ul')`.
    [jsPerf](http://jsperf.com/jquery-find-vs-context-sel/16)

### 25.4 Scoped jQuery Object Queries

-   Use `find` with scoped jQuery object queries.

```js
// bad
$('ul', '.sidebar').hide();

// bad
$('.sidebar').find('ul').hide();

// good
$('.sidebar ul').hide();

// good
$('.sidebar > ul').hide();

// good
$sidebar.find('ul').hide();
```

ECMAScript 5 Compatibility
--------------------------

### 26.1 Compatibility Table

-   Refer to [Kangax](https://twitter.com/kangax/)'s ES5 [compatibility
    table](http://kangax.github.io/es5-compat-table/).

ECMAScript 6 Styles
-------------------
### 27.1 Links to ES6 Features

1.  [Arrow Functions](#arrow-functions)
2.  [Classes](#constructors)
3.  [Object Shorthand](#creating-objects-with-methods)
4.  [Object Concise](#property-value-shorthand)
5.  [Object Computed
    Properties](#creating-objects-with-dynamic-property-names)
6.  [Template Strings](#building-strings)
7.  [Destructuring](#destructuring)
8.  [Default Parameters](#specifying-default-values-for-arguments)
9.  [Rest](#accepting-a-dynamic-number-of-arguments)
10. [Array Spreads](#copying-arrays)
11. [Let and Const](#references)
12. [Iterators and Generators](#iterators-and-generators)
13. [Modules](#modules)

Testing
-------
### 28.1 Should I Write Tests?

-   **Yup.**

```js
function foo() {
  return true;
}
```

### 28.2 No, but Seriously

-   Whichever testing framework you use, you should be writing tests!
-   Strive to write many small pure functions, and minimize where
    mutations occur.
-   Be cautious about stubs and mocks â€“ they can make your tests more
    brittle.
-   We primarily use
    [ember-cli-qunit](https://www.npmjs.com/package/ember-cli-qunit) at
    LinkedIn.
-   100% test coverage is a good goal to strive for, even if it's not
    always practical to reach it.
-   Whenever you fix a bug, *write a regression test*.
-   A bug fixed without a regression test is almost certainly going to
    break again in the future.

Performance
-----------
-   [On Layout & Web
    Performance](http://www.kellegous.com/j/2013/01/26/layout-performance/)
-   [String vs Array Concat](http://jsperf.com/string-vs-array-concat/2)
-   [Try/Catch Cost In a Loop](http://jsperf.com/try-catch-in-loop-cost)
-   [Bang Function](http://jsperf.com/bang-function)
-   [jQuery Find vs Context,
    Selector](http://jsperf.com/jquery-find-vs-context-sel/13)
-   [innerHTML vs textContent for script
    text](http://jsperf.com/innerhtml-vs-textcontent-for-script-text)
-   [Long String Concatenation](http://jsperf.com/ya-string-concat)
-   Loading...

Resources
---------
### Reading for New UI Engineers

-   [Basic JavaScript for the impatient
    programmer](http://www.2ality.com/2013/06/basic-javascript.html)
    -Dr. Axel Rauschmayer
-   [Frontend Guidelines](https://github.com/bendc/frontend-guidelines)
    -Benjamin De Cock

### Learning ES6

-   [Overview of ES6 Features](https://github.com/lukehoban/es6features)
-   [Exploring ES6 Book](http://exploringjs.com/es6/)

### Further Reading

-   [Standard
    ECMA-262](http://www.ecma-international.org/ecma-262/6.0/index.html)
-   [Latest ECMA-262 Spec](https://tc39.github.io/ecma262/)

### Tools

-   [ESLint](http://eslint.org/)

### Other Style Guides

-   [Airbnb Javascript Style Guide](http://airbnb.io/javascript/) â€“ the
    point of origin for this guide

### Books

-   [Superhero.js](http://superherojs.com/) - Kim Joar Bekkelund, Mads
    MobÃ¦k, & Olav Bjorkoy
-   [JSBooks: free JavaScript
    resources](http://jsbooks.revolunet.com/) - Julien Bouquillon
-   [You Don't Know JS (book
    series)](https://github.com/getify/You-Dont-Know-JS) - Kyle Simpson

### Blogs

-   [2ality](http://www.2ality.com/) - Axel Rauschmayer
-   [QuirksBlog](http://www.quirksmode.org/blog/)
-   [Addy Osmani](https://addyosmani.com/blog/)
-   [Mozilla Hacks](https://hacks.mozilla.org/)
-   [Chromium Blog](http://blog.chromium.org/)

### Newsletters

-   [JavaScript Weekly](http://javascriptweekly.com/)
-   [Ember Weekly](http://emberweekly.com/)
-   [ES.next News](http://esnextnews.com/)

Amendments
----------

We encourage you to fork this guide and change the rules to fit your
team's style guide. Below, you may list some amendments to the style
guide. This allows you to periodically update your style guide without
having to deal with merge conflicts.


## JSDocs

* Methods should be documented following JSDocs conventions, more information at [usejsdoc.org](usejsdoc.org) Inline comments are encouraged and should be used to explain code.
