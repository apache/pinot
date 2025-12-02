# MkDocs tips

This page contains some tips for writing documentation using MkDocs.

[TOC]

## Abbreviations

Abbreviations are a common way to shorten words or phrases, but usually require a definition to be understood.
Instead of keep repeating the definition, you can use Markdown abbreviations.
There is documentation about them in the [MkDocs documentation][mkdocs_abbreviations] and the
[abbreviation extension][mkdocs_abbreviations_extension].

The TL;DR is that you can define abbreviations in a Markdown as:

```markdown
*[<abbreviation>]: <Meaning>
```

Where `<abbreviation>` is the abbreviation and `<Meaning>` is the definition of the abbreviation that will be
shown as a tooltip when the abbreviation is hovered.
As always, it is recommended to add these definitions in the end of the current section or file.

Some aggregations are widely used in Pinot.
In order to do not have to repeat the definition of these aggregations, we can use a glossary.
The glossary is defined in `/mkdocs/includes/abbreviations.md` and it is imported automatically in all documentation 
pages.

Currently defined abbreviations are:
```markdown
--8<-- "mkdocs/includes/abbreviations.md:abbreviations"
```

[mkdocs_abbreviations]: https://squidfunk.github.io/mkdocs-material/setup/extensions/python-markdown/#abbreviations
[mkdocs_abbreviations_extension]: https://python-markdown.github.io/extensions/abbreviations/

## Snippets

Snippets are a way to include a file in a Markdown file.
This is specially useful in developer documentation, where we can include code snippets from the codebase.
You can read more about this extension in [snippets extension] documentation page, although here is a quick summary
of the usage.

Snippets can be added as a single line or as a block.
The syntax for a single line snippet is:

```markdown
;--8<-- "path/to/file"
```

And for a block snippet:

```markdown
;--8<--
path/to/file1
path/to/file2
;--8<--
```

Snippets can also be configured to include specific lines on the file by providing a range. The syntax is 
`path/to/file:start_line:end_line` and some extra options are available in the [snippets extension] documentation.
But including specific lines is not recommended, as it can make the documentation outdated if the code changes.
Instead, the extension supports adding marks in the source file to include only the code between these marks.
How marks are added depends on the language, but it is usually following the comment syntax of the language.

For example, given a class like:

```java
public class MyClass {
   public static void myFunction() {
      System.out.println("Hello, World!");
   }
} 
```

In order to include the `myFunction` method in a markdown file, you can add do the following:

1.  Modify the Java file to include marks. For example:

    ```java
    public class MyClass {
       // --8<-- [start:myFunction]
       public static void myFunction() {
          System.out.println("Hello, World!");
       }
       // --8<-- [end:myFunction]  
    } 
    ```
2.  Include the snippet in the Markdown file:

    ~~~markdown
    ```java
    ;--8<-- "path/to/MyClass.java:myFunction"
    ```
    ~~~

    Remember to wrap the snippet in a code block to get the syntax highlighting.
    The snippets extension is run as a preprocessor, so if a snippet is found in a fenced code block etc., 
    it will still get processed.

By using this approach, the documentation will always be up to date with the codebase, even if the code changes.
It is also useful to know that a file is being included in the documentation. 

[snippets extension]: https://facelessuser.github.io/pymdown-extensions/extensions/snippets/#snippet-sections

## Mermaid diagrams

Mermaid is a tool to create diagrams and flowcharts from text that is integrated with most tools including MkDocs
but also GitHub, Google Docs, etc.
You can read more about it in the [Mermaid documentation][mermaid].

In order to use Mermaid in a Markdown file, you need to wrap the Mermaid code in a code block with the language 
`mermaid`.
For example the following code block:

~~~markdown
```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```
~~~

Is rendered as

```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```

[mermaid]: https://mermaid.js.org/intro/
