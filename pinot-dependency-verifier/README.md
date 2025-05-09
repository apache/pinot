# Pinot Dependency Verifier

This module implements a custom Maven Enforcer plugin rule that validates dependency declarations in the Apache Pinot
project. It enforces internal
[Dependency Management Guidelines](https://docs.pinot.apache.org/developers/developers-and-contributors/dependency-management)
by checking for hardcoded versions and misplaced dependencies.

## Building the Module
From the repo root, build and install only `pinot-dependency-verifier` without triggering the verification, 
so the artifact is installed into the local Maven repository:

   ```bash
   mvn clean install \
      -pl pinot-dependency-verifier \
      -am \
      -DskipTests \
      -Drun.dependency.verifier=false
   ```

Then, to build the full Pinot project with verification enabled:

   ```bash
   mvn clean verify \
      -Pbin-dist,dependency-verifier \
      -Drun.dependency.verifier=true \
      -DskipTests
   ```

## Running the Plugin

To manually run the enforcer plugin across the entire Pinot codebase:

```bash
mvn enforcer:enforce -Pdependency-verifier -Drun.dependency.verifier=true
```

## Developing & Testing
To run the unit tests:

   ```bash
   mvn test
   ```