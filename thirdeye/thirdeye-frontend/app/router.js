import EmberRouter from '@ember/routing/router';
import config from './config/environment';
import Piwik from 'ember-cli-piwik/mixins/page-view-tracker';

const Router = EmberRouter.extend(Piwik, {
  location: config.locationType,
  rootURL: config.rootURL
});

Router.map(function() {
  this.route('login');
  this.route('logout');
  //page not found placeholder - lohuynh
  //this.route('404', { path: '/*path' });

  this.route('home', function() {
    this.route('index', { path: '/' });
    this.route('share-dashboard');
  });

  this.route('aiavailability');

  this.route('anomalies');

  this.route('manage', function() {
    this.route('alert', { path: 'alert/:alert_id' }, function() {
      this.route('explore');
      this.route('tune');
      this.route('edit');
    });
    this.route('alerts', function() {
      this.route('performance');
    });
    this.route('explore', { path: 'explore/:alert_id'});
    this.route('yaml', { path: 'yaml/:alert_id' });
  });

  this.route('self-serve', function() {
    this.route('create-alert');
    this.route('import-metric');
    this.route('import-sql-metric');
  });

  this.route('screenshot', { path: 'screenshot/:anomaly_id' });
  this.route('rootcause');
  this.route('preview');
  this.route('auto-onboard');
});

export default Router;
