import Ember from 'ember';
import config from './config/environment';

const Router = Ember.Router.extend({
  location: config.locationType,
  rootURL: config.rootURL
});

Router.map(function() {
  this.route('example', { path: 'example/:id' });

  this.route('manage', function() {
    this.route('alerts');
  });

  this.route('rca', function() {
    this.route('metrics', { path: '/metrics/:id' });
  });
  this.route('self-serve', function() {
    this.route('create-alert');
    this.route('import-metric');
  });
});

export default Router;
