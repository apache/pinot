/* eslint-env node */
'use strict';

const EmberApp = require('ember-cli/lib/broccoli/ember-app');
const Funnel = require('broccoli-funnel');
const MergeTrees = require('broccoli-merge-trees');

module.exports = function(defaults) {
  let app = new EmberApp(defaults, {
    // Add options here
    'ember-cli-babel': {
      includePolyfill: true
    },

    fingerprint: {
      prepend: '/app/'
    },

    sassOptions: {
      extension: 'scss',
    },

    sourcemaps: {
      enabled: EmberApp.env() !== 'production',
      extensions: ['js', 'css']
    },

    babel: {
      sourceMaps: 'inline',
      plugins: [
        'transform-object-rest-spread'
      ]
    },

    'ember-bootstrap': {
      'bootstrapVersion': 3,
      'importBootstrapFont': true,
      'importBootstrapCSS': false
    },

    //for ember EDITOR
    ace: {
      themes: ['ambiance', 'chaos'],
      modes: ['yaml'],
      exts: ['language_tools']
    }
  });

  const sourceSansProFontTree = new Funnel(app.bowerDirectory + '/source-sans-pro', {
    srcDir: '/',
    include: ['**/*.woff2', '**/*.woff', '**/*.ttf'],
    destDir: '/assets'
  });

  app.import(app.bowerDirectory + '/source-sans-pro/source-sans-pro.css');
  //ionRangeSlider assets - http://ionden.com/a/plugins/ion.rangeSlider/en.html
  app.import('node_modules/ion-rangeslider/css/ion.rangeSlider.skinHTML5.css');
  // app.import('node_modules/ion-rangeslider/img/sprite-skin-nice.png', {
  //   destDir: 'img'
  // });

  app.import({
    production: 'node_modules/ion-rangeslider/js/ion.rangeSlider.min.js',
    development: 'node_modules/ion-rangeslider/js/ion.rangeSlider.js'
  });
  app.import('node_modules/ion-rangeslider/css/ion.rangeSlider.css');

  // jspdf and html2canvas assets for PDF
  app.import('node_modules/jspdf/dist/jspdf.min.js');

  // Use `app.import` to add additional libraries to the generated
  // output files.

  // If you need to use different assets in different
  // environments, specify an object as the first parameter. That
  // object's keys should be the environment name and the values
  // should be the asset to use in that environment.
  //
  // If the library that you are including contains AMD or ES6
  // modules that you would like to import into your application
  // please specify an object with the list of modules as keys
  // along with the exports of each module as its value.
  // return app.toTree();

  return app.toTree(new MergeTrees([sourceSansProFontTree]));
};
