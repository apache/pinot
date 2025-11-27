# Project overview
Originally developed at LinkedIn, Apache PinotTM is a real-time distributed OLAP datastore,
purpose-built to provide ultra low-latency analytics at extremely high throughput.

With its distributed architecture and columnar storage, Apache Pinot empowers businesses to gain valuable insights from
real-time data, supporting data-driven decision-making and applications.

This document guides AI coding assistants (Copilot, Cursor, etc.) contributing to this repository.

## Libraries and Frameworks

- Most source code is written in Java 11.
- Code in `pinot-clients` is written in Java 8.
- Pinot UI is a React.js frontend. It is stored in `pinot-controller/src/main/resources/`.
- The code is compiled with Maven and follows a multimodule structure. Use each module's `pom.xml` to
  understand dependencies and configurations.

## Coding Standards

- Include the Apache Software Foundation (ASF) license header in all supported file types (Java, XML, JSON, Markdown).
- Javadoc comments can either be written using the `/** ... */` syntax or the `///` syntax,
  as specified in [JEP-467](https://openjdk.org/jeps/467).
- Methods and parameters are non-null by default unless annotated with `javax.annotation.Nullable`.
- Prefer explicit imports for classes rather than fully qualified names used inline.
- Use SLF4J for logging; do not use `System.out` or `System.err`.
- Propagate exceptions with useful context; avoid blanket `catch (Exception)` and never swallow errors.
- Keep diffs minimal: do not reformat unrelated code and preserve existing whitespace/indentation styles.
- Favor early returns and guard clauses; avoid deep nesting beyond two to three levels.
- Add concise comments that explain "why", not "how"; avoid TODOs—implement instead.

## Repository and Workflow Conventions

- Use SSH Git remotes (`git@github.com:...`) rather than HTTPS.
- Keep changes small and focused; include or update tests when behavior changes.
- Write descriptive commit messages and PR descriptions; link related issues when relevant.
- Prefer absolute file paths in discussions and tool calls for clarity.
- Do not commit generated files, large binaries, or secrets.

## Build and Test

- Build with Maven from the repository root.
- Run unit tests locally before opening a PR; ensure the project compiles cleanly without introducing lint errors.
- Respect the multi-module structure; build order is governed by Maven.

## Domain-specific conventions

- Timezones: When defining or updating `STANDARD_TIMEZONES`, use a three-letter timezone ID with location in the
  format `PDT(America/Los_Angeles)`. Prefer specific timezone IDs over generic UTC offsets, ensure comprehensive
  coverage of common timezones, and sort entries by UTC offset.
- Imports: Favor importing classes at the top of files instead of using fully qualified class names inline.

## Testing Best Practices

- Write unit tests for new functionality and bug fixes; follow the existing test naming conventions.
- Use Mockito for mocking dependencies in unit tests.
- Include integration tests when modifying core components or data flows.
- Add performance tests for optimization changes; measure impact with realistic data volumes.
- Use `@Test(expected = Exception.class)` sparingly; prefer `@Test` with try-catch for specific assertions.
- Keep test methods focused and descriptive; avoid overly complex test setups.

## Test Coverage Guidelines

- Aim for high branch coverage (≥80%) on new code and modified code paths.
- Focus on testing business logic rather than trivial getters/setters or framework boilerplate.
- Prioritize coverage of error handling paths, edge cases, and boundary conditions.
- Use JaCoCo or similar tools for measuring coverage; include coverage reports in CI builds.
- Don't artificially inflate coverage with trivial tests; quality matters more than quantity.
- Cover both happy path and failure scenarios, including timeout and resource exhaustion cases.
- For critical components (e.g., query execution, data ingestion), maintain ≥90% coverage.
- Document coverage expectations in PR descriptions when coverage goals aren't met.
- Use mutation testing (e.g., PITest) for critical code to ensure test quality.
- Exclude generated code, test utilities, and simple data transfer objects from coverage requirements.

## Performance Considerations

- Be mindful of memory usage in data processing pipelines; avoid loading large datasets into memory unnecessarily.
- Use efficient data structures (e.g., primitive arrays over collections for numerical data).
- Consider lazy evaluation for expensive operations that might not always be needed.
- Profile performance-critical code paths and document assumptions about data scale.
- Use appropriate collection types based on access patterns (e.g., ArrayList for iteration, HashMap for lookups).

## Security Guidelines

- Never log sensitive information like passwords, API keys, or personally identifiable data.
- Validate all user inputs and external data sources; sanitize inputs to prevent injection attacks.
- Use parameterized queries for database operations to prevent SQL injection.
- Implement proper authentication and authorization checks for sensitive operations.
- Follow the principle of least privilege when granting permissions.

## Error Handling and Logging

- Use appropriate log levels: ERROR for failures, WARN for concerning situations, INFO for important events, DEBUG for detailed troubleshooting.
- Include relevant context in log messages (e.g., request IDs, user context, operation parameters).
- Use structured logging with key-value pairs for better searchability and analysis.
- Create custom exception types for domain-specific errors rather than generic RuntimeException.
- Include stack traces only in DEBUG level; avoid in production logs to prevent information leakage.

## Concurrency and Threading

- Use thread-safe data structures (e.g., ConcurrentHashMap) in multi-threaded contexts.
- Prefer immutable objects for shared state to avoid race conditions.
- Use appropriate synchronization mechanisms; favor higher-level constructs over synchronized blocks.
- Document thread safety guarantees in class-level Javadoc.
- Be aware of Pinot's distributed nature; consider consistency models and eventual consistency scenarios.

## Configuration Management

- Externalize configuration values; avoid hardcoding environment-specific settings.
- Use meaningful default values with clear documentation about their impact.
- Validate configuration at startup and provide helpful error messages for invalid settings.
- Document all configuration properties in code comments or separate configuration docs.
- Use consistent naming conventions for configuration keys (e.g., pinot.server.port).

## Documentation Standards

- Document public APIs with comprehensive Javadoc including parameter descriptions, return values, and exceptions.
- Include code examples in documentation where helpful for complex APIs.
- Update documentation when changing behavior; keep README files current.
- Use consistent terminology across documentation and code comments.
- Document performance characteristics and limitations for important components.

## Code Review Process

- Address all review comments thoughtfully; if you disagree, explain your reasoning clearly.
- Keep PRs focused on a single concern; split large changes into smaller, reviewable commits.
- Include context in PR descriptions: what problem you're solving, how you solved it, and any trade-offs.
- Test your changes thoroughly before requesting review; include test results when relevant.
- Be responsive to feedback and iterate quickly on requested changes.

## Assistant guidance

- When editing, touch only what is necessary; keep changes minimal and localized.
- Preserve the file's existing indentation and whitespace; do not reflow or reformat unrelated code.
- When creating new files, include the ASF license header and all required imports/dependencies.
- Avoid adding extremely long hashes or non-textual content; keep repository contents textual and reviewable.
- If uncertain about where a change belongs, search for similar code in the codebase and follow established patterns.
- Use absolute file paths in discussions and tool calls for clarity.
- Prefer early returns and guard clauses to reduce nesting and improve readability.
- When implementing complex logic, break it into smaller, well-named methods.
- Consider edge cases and failure scenarios in your implementations.
- Follow the existing code patterns and architectural decisions in the codebase.
