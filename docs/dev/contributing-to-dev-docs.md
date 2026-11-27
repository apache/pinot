---
title: Contributing to the developer documentation
summary: >-
  Rules for pages under docs/dev: the page template, the Markdown features that are allowed, how to build the site,
  and how to keep a page correct when the code changes.
verified_at: 5b53f30d80
---

# Contributing to the developer documentation

## Audience

These pages have two kinds of readers:

- people who start to work on a part of Pinot;
- coding agents that read the raw Markdown files in a checkout.

Write for both.
A page must be correct and complete as raw Markdown.
The [Zensical](https://zensical.org) site is a convenience, not the source of truth.

## Where a page goes

| Content | Location |
|---|---|
| How a part of Pinot works: components, data flow, invariants | `docs/dev/` (these pages) |
| How to do a task: build, test, review, benchmark | `kb/skills/` |
| How to use or configure Pinot | The user documentation at [docs.pinot.apache.org](https://docs.pinot.apache.org) |

Do not copy content between these locations.
Link to it.

## Page template

Each page has YAML front matter and the sections below, in this order.
Leave out a section only if it has no content.

```markdown
---
title: <Name of the topic>
summary: >-
  <One to three sentences. What the page answers. An agent reads only this field to decide if it needs the page.>
modules: [<maven modules>]
key_classes:
  - <fully qualified class name>
related: [<other pages, relative to docs/dev>]
verified_at: <short commit hash>
---

# <Name of the topic>

## TL;DR
<Three to six bullets. The facts that a reader must know.>

## <Flow or structure>
<A mermaid diagram, then the same content as a numbered list.>

## Where it is in the code
<Table: class, module, role.>

## Configs and query options
<Table: name, kind, default, effect.>

## Invariants
<Rules that the code must keep. A change that breaks one is a bug.>

## Gotchas
<Behavior that surprises readers, or that is easy to break.>
```

## Allowed Markdown features

Use only features that show the same content on GitHub, in a text editor, and in Zensical:

| Feature | Allowed | Reason |
|---|---|---|
| Tables, lists, headings, links, code blocks | Yes | Standard Markdown. |
| Mermaid diagrams in a `mermaid` code block | Yes | GitHub and Zensical render them. Agents read the source. |
| YAML front matter | Yes | GitHub shows it as a table. Zensical reads it as page metadata. |
| Admonitions (`!!! note`) | No | GitHub shows the body as a code block. Write `> **Note:** ...` instead. |
| Abbreviation tooltips (`*[MSE]: ...`) | No | Raw readers do not see the definition. Use the [glossary](glossary.md). |
| Snippets (`--8<--`) | No | Raw readers see only the include line. Reference the class and method instead. |
| Content tabs | No | Raw readers see a flat list without context. |

## Diagrams

Use a diagram when the reader must see a flow, a sequence, or a structure.

| Content | Mermaid type |
|---|---|
| Order of calls between classes | `sequenceDiagram` |
| Decisions and data flow | `flowchart` |
| Life cycle of an object | `stateDiagram-v2` |
| Parts of a component | `flowchart` or `classDiagram` |

After each diagram, write the same content as a numbered list or a table.
A reader that cannot render the diagram must not lose information.

Use real class and method names in the diagram nodes.

## References to code

- Write class names, method names, configs, and query options in backticks.
- Use the simple class name in the text, and the fully qualified name in `key_classes`.
- Do not give line numbers. They change too often.
- Do not paste large blocks of code. Name the method that the reader must open.

## Keep pages correct

A wrong page is worse than no page, because readers trust it.

- Update a page in the same pull request that changes the behavior it describes.
- When you compare a page with the code, set `verified_at` to the short hash of the commit that you compared it with.
- If you are not sure about a fact, read the code before you write it.
  Do not copy facts from old design documents or issues.
- Write the current behavior only. Do not write history or plans.

## Language

Write in the style of Simplified Technical English:

- one fact in each sentence, and not more than 25 words;
- simple present tense and active voice;
- "must" for requirements, and no "should", "may", or "might";
- one term for one concept, as the [glossary](glossary.md) defines it.

## Build the site

The site uses [Zensical](https://zensical.org). The configuration is in `zensical.toml` at the root of the repository.
Run these commands from the root of the repository.
Use [uv](https://docs.astral.sh/uv/) or Docker.
Without either, run `pip install zensical==0.1.1`, then run `zensical` directly.

Serve the site with live reload on <http://localhost:8000>:

```bash
# With uv
uvx zensical==0.1.1 serve
# With Docker
docker run --rm -it -p 8000:8000 -v "$(pwd)":/docs zensical/zensical:0.1.1
```

Build the site into `site/`. The build fails on warnings, for example on broken links or anchors:

```bash
# With uv
uvx zensical==0.1.1 build --strict
# With Docker
docker run --rm -v "$(pwd)":/docs zensical/zensical:0.1.1 build --strict
```

The CI workflow `.github/workflows/pinot_dev_docs.yml` uses the same Zensical version.
When you change the version, change it in the workflow, in `zensical.toml`, and on this page.
