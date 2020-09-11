# Website

This website is built using [Docusaurus 2](https://v2.docusaurus.io/), a modern static website generator.

NOTE: You need to install [YARN](https://classic.yarnpkg.com/en/docs/install)

## Installation

```bash
$ yarn
```

### Local Development

```bash
$ yarn start
```

Link Check:

```bash
yarn lint-check
-> All matched files use Prettier code style!
```

Then try out:

```bash
yarn run serve --build --port 3001 --host 0.0.0.0
```

This command starts a local development server and open up a browser window. Most changes are reflected live without having to restart the server.

### Build

```bash
$ yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Deployment

```bash
$ GIT_USER=[USER_ID] USE_SSH=true yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.
