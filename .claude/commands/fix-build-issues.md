# Fix Build Issues

## Description
Incrementally diagnose and fix Maven build issues by progressing through compilation stages. This command takes a cautious approach, avoiding expensive operations like `mvn clean` and stops to ask for guidance when code compilation issues are found. Uses parallel compilation and output redirection to avoid context pollution.

## Usage
/fix-build-issues [module-path]

## Arguments
- `module-path`: (Optional) Specific Maven module to focus on. If not provided, works on the entire project.

## Examples
/fix-build-issues
/fix-build-issues pinot-common
/fix-build-issues pinot-core

## Implementation

I'll help you incrementally fix Maven build issues by progressing through compilation stages carefully. I will:

1. **Never run `mvn clean`** unless explicitly requested as a last resort
2. **Stop and ask for guidance** if I encounter code compilation errors
3. **Focus on build configuration issues** rather than source code fixes
4. **Progress incrementally** through Maven lifecycle phases
5. **Use parallel compilation** (`-T 1C`) for faster builds
6. **Redirect verbose output** (`-l logfile`) to avoid context pollution

Let me start by checking the current build status:

**Phase 1: Light Formatting and License Check**
- Run `mvn license:format spotless:apply` (light operations, no redirection needed)
- These are fast enough to run without output redirection

**Phase 2: Basic Compilation Check**
- Run `mvn compile -DskipTests -T 1C -l compile.log` to check basic compilation with parallel processing
- Analyze `compile.log` file for compilation errors
- Only show relevant errors/warnings, not the full verbose output

**Phase 3: Dependency Resolution**
- Run `mvn dependency:tree -l dependency.log` and analyze the output file
- Check for dependency conflicts without polluting context
- Verify Maven configuration files (pom.xml) for obvious issues

**Phase 4: Full Build Attempt**
- Attempt `mvn install -DskipTests -T 1C -l install.log` to build without running tests (DO NOT RUN TESTS!)

**Throughout the process:**
- Use `-T 1C` for parallel compilation when possible
- Use `-l <logfile>` to redirect verbose Maven output to files
- Read and analyze log files to extract relevant information
- Monitor for compilation errors and stop immediately if found
- Focus on configuration issues (Maven settings, dependencies, plugins)
- Avoid making source code changes
- Provide clear status updates at each phase

**Maven Command Pattern:**
```bash
mvn <goal> -T 1C -l <goal>.log [other-options]
```

**If compilation errors are found:**
- Stop the process immediately
- Extract and report specific files and errors from log files
- Ask for user guidance on how to proceed
- Do not attempt to fix source code automatically

**Common build issues I can help with:**
- Maven plugin configuration problems
- Dependency version conflicts
- Missing build resources
- Incorrect Maven profiles
- Build lifecycle configuration issues
