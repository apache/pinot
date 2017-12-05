/* eslint-env node */
'use strict';

module.exports = function(environment) {

  let ENV = {

    appName: 'ThirdEye',

    modulePrefix: 'thirdeye-frontend',

    environment: environment,

    podModulePrefix: 'thirdeye-frontend/pods',

    rootURL: '/app/',

    locationType: 'hash', 'ember-cli-mirage': {
      directory: 'app/mirage'
    },

    moment: {
      includeTimezone: 'all'
    },

    EmberENV: {

      FEATURES: {
        // Here you can enable experimental features on an ember canary build
        // e.g. 'with-controller': true
      },

      EXTEND_PROTOTYPES: {
        // Prevent Ember Data from overriding Date.parse.
        Date: false
      }
    },

    'ember-simple-auth':  {
      baseURL: '/app/#/rca'
    },

    APP: {
      // you can pass flags/options to your application instance
      // when it is created
    }
  };

  if (environment === 'development') {
    ENV.rootURL = '/';
    ENV['ember-simple-auth'] = {
      baseURL: '/#/rca'
    };
    // necessary for local development
    // ENV.APP.LOG_ACTIVE_GENERATION = true;
    // ENV.APP.LOG_TRANSITIONS = true;
    // ENV.APP.LOG_TRANSITIONS_INTERNAL = true;
    // ENV.APP.LOG_VIEW_LOOKUPS = true;
  }

  if (environment === 'test') {
    ENV.rootURL = '/';

    // Testem prefers this...
    ENV.locationType = 'none';

    // keep test console output quieter
    ENV.APP.LOG_ACTIVE_GENERATION = false;
    ENV.APP.LOG_VIEW_LOOKUPS = false;

    ENV.APP.rootElement = '#ember-testing';
  }

  if (environment === 'production') {}

  return ENV;
};
