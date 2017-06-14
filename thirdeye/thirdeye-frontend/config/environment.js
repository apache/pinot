/* eslint-env node */

module.exports = function(environment) {

  var ENV = {

    appName: 'ThirdEye',

    modulePrefix: 'thirdeye-frontend',

    environment: environment,

    podModulePrefix: 'thirdeye-frontend/pods',

    rootURL: '/app/',

    locationType: 'hash', 'ember-cli-mirage': {
      directory: 'app/mirage'
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

    APP: {
      // you can pass flags/options to your application instance
      // when it is created
    },

    navigation: {
      /**
       * Array of global primary menu items
       */
      globalNav: [
        {
          className: 'home',
          link: 'index',
          title: 'Home'
        },
        {
          className: 'anomalies',
          link: '/1426/thirdeye',
          isCustomLink: true,
          title: 'Anomalies'
        },
        {
          className: 'rca',
          link: 'example',
          id: '1',
          title: 'Root Cause Analysis'
        },
        {
          className: 'alert',
          link: 'self-service',
          title: 'Self-Service'
        }
      ],

      /**
       * Array of 'self-service' sub navigation items
       */
      selfServiceNav: [
        {
          className: 'menu-item--manage',
          link: 'self-service.manage',
          title: 'Manage Alerts'
        },
        {
          className: 'menu-item--create',
          link: 'self-service.create',
          title: 'Create Alert'
        },
        {
          className: 'menu-item--onboard',
          link: 'self-service.onboard',
          title: 'Onboard Metric'
        }
      ]
    }
  };

  if (environment === 'development') {
    ENV.rootURL = '/';
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
