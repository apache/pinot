# How to build dev doc

[TOC]

In order to build the documentation, you need to either install MkDocs, Material for MkDocs and a bunch of Python
packages or just use the Docker image.

## Build the static site using MkDocs
Here are the steps to build the documentation using Docker:

1. Go to the root of the Pinot repository.
2. Run the following command:
```bash
docker run \
  --user $UID:$GID \
  --rm \
  -v $(pwd):/docs \
  squidfunk/mkdocs-material \
  build \
  --strict
```

The documentation will be generated in the `site` directory.
The `--strict` flag is optional and will make the build fail if there are any warnings, including when there are broken
links.

## Serve the documentation locally
While developing the documentation, you can serve the documentation locally.
This will start a local server that will automatically reload the documentation when you make changes to the source
files.

Here are the steps to serve the documentation using Docker:

1. Go to the root of the Pinot repository.
2. Run the following command:
```bash
docker run \
  --user $UID:$GID \
  --rm \
  -v $(pwd):/docs \
  -p 8000:8000 \
  squidfunk/mkdocs-material
```

The documentation will be available at [http://localhost:8000](http://localhost:8000).
