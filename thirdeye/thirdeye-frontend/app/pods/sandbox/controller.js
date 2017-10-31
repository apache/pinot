import Ember from 'ember';
import moment from 'moment';
import fetch from 'fetch';

const generateRandomData = (num, min=0, max=100) => {
  const diff = max - min;
  const dataPoints = [];
  for (let i = 0; i < num; i++) {
    dataPoints.push(Math.random() * diff + min);
  }
  return dataPoints;
};

const generateSequentialData = (num, min=0, max=100) => {
  const diff = max - min;
  const dataPoints = [];
  for (let i = 0; i < num; i++) {
    dataPoints.push(min + diff * (i / num));
  }
  return dataPoints;
};

export default Ember.Controller.extend({

  types: null,

  colors: null,

  axes: null,

  series: Ember.computed(
    'model.data',
    'types',
    'colors',
    'axes',
    function() {
      console.log('series()');

      const types = this.get('types') || {};
      const colors = this.get('colors') || {};
      const axes = this.get('axes') || {};
      const data = this.get('model.data') || {};
      console.log('data', data);

      const series = {};
      Object.keys(data).forEach(range =>
        Object.keys(data[range]).filter(s => s != 'timestamp').forEach(mid => {
          const sid = range + '-' + mid;
          series[sid] = {
            timestamps: data[range]['timestamp'],
            values: data[range][mid],
          };

          if (sid in types) {
            series[sid].type = types[sid];
          }

          if (sid in colors) {
            series[sid].color = colors[sid];
          }

          if (sid in axes) {
            series[sid].axis = axes[sid];
          }
      }));

      console.log('series', series);
      return series;
    }
  ),

  actions: {
    addSeries(sid) {
      console.log('addSeries()');

      const data = this.get('model.data');

      data['myrange'] = {
        [sid]: generateRandomData(100, 0, 20000), "timestamp": generateSequentialData(100, 1508454000000, 1508543940000)
      };

      console.log('data', data);
      this.set('model.data', data);
      this.notifyPropertyChange('model');
    },

    removeSeries() {
      console.log('removeSeries()');

      const data = this.get('model.data');

      delete data['myrange'];

      console.log('data', data);
      this.set('model.data', data);
      this.notifyPropertyChange('model');
    },

    changeType(sid, type) {
      console.log('changeTypes()');

      const types = this.get('types') || {};

      types['myrange-' + sid] = type;

      console.log('types', types);
      this.set('types', types);
      this.notifyPropertyChange('types');
    },

    changeColor(sid, color) {
      console.log('changeColor()');

      const colors = this.get('colors') || {};

      colors['myrange-' + sid] = color;

      console.log('colors', colors);
      this.set('colors', colors);
      this.notifyPropertyChange('colors');
    },

    changeAxis(sid, axis) {
      console.log('changeAxis()');

      const axes = this.get('axes') || {};

      axes['myrange-' + sid] = axis;

      console.log('axes', axes);
      this.set('axes', axes);
      this.notifyPropertyChange('axes');
    }
  }
});
