
//takes utc date iso format ie. 2016-04-06T07:00:00.000Z, returns date and time in users timezone
Handlebars.registerHelper('displayDate', function (date) {
  var tz = getTimeZone();
  // return moment(date).tz(tz).format('YYYY-MM-DD h a z');
  return moment(date).tz(tz).format('YYYY-MM-DD');
});

Handlebars.registerHelper('formatPercent', function (percentValue) {
  if(percentValue >= 0){
    return "+" + percentValue.toFixed(1) +"%";
  } else {
    return "" + percentValue.toFixed(1) + "%";
  }
});

Handlebars.registerHelper('formatDouble', function (doubleValue) {
  var value = Number(doubleValue);
  return Number.isNaN(value) ? '' : d3.format('.3r')(doubleValue);
});

Handlebars.registerHelper('formatDelta', function (a, b) {
  delta = a - b;
  if(delta >= 0){
    return "+" + delta.toFixed(1);
  } else {
    return "" + delta.toFixed(1);
  }
});

/**
 * Displays human readable number
 * @param {number} num A number
 * @return {string} human readable number
 */
Handlebars.registerHelper('formatNumber', function(num) {
  return num.toString().replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
});

/**
 * Template helper that assigns the correct css class based on the delta
 * @param  {string} value string value of delta ('2.4%')
 * @return [string] css class positive/negative
 */
Handlebars.registerHelper('colorDelta', function(value) {
    if (value ===  'N/A') { return; }
    return parseInt(value) >= 0 ? 'positive' : 'negative';
});

Handlebars.registerHelper('displayHour', function (date) {
  var tz = getTimeZone();
  return moment(date).tz(tz).format('h a');
});

Handlebars.registerHelper('displayMonthDayHour', function (date) {
  var tz = getTimeZone();
  return moment(date).tz(tz).format('M/D hA');
});

/**
 * Displays human readable date
 * @param {string} granularity granularity of the metric
 * @param {string} date A date
 * @return {string} human readable date
 */
Handlebars.registerHelper('formatDate', function (granularity, date) {
  var tz = getTimeZone();
  const dateFormat = {
    'DAYS': 'M/D',
    'HOURS': 'M/D ha',
    '5_MINUTES': 'M/D hh:mm a'
  }[granularity];

  return moment(date).tz(tz).format(dateFormat);
});

Handlebars.registerHelper('if_eq', function(a, b, opts) {
  if (a == b) {
      return opts.fn(this);
  } else {
      return opts.inverse(this);
  }
});

Handlebars.registerHelper('truncate', function(str, maxlen) {
  if (str.length > maxlen) {
    return str.substr(0, maxlen-3) + "...";
  } else {
    return str
  }
});

Handlebars.registerHelper('if_no_anomalies', function(info, opts) {
  if( info.open == 0 && info.resolved == 0) {
    return opts.fn(this)
  } else{
    return opts.inverse(this);
  }

});

Handlebars.registerHelper('computeColor', function(value, inverseMetric) {
 const opacity = Math.abs(value / 25);
 if((value > 0 && !inverseMetric) || (value < 0 && inverseMetric)) {
   return "rgba(0,0,234," + opacity + ")";
 } else {
   return "rgba(234,0,0,"  + opacity + ")";
 }
});

//compute the text color so that its visible based on background
Handlebars.registerHelper('computeTextColor', function(value) {
  const opacity = Math.abs(value/25);
  if(opacity < 0.5){
    return "#000000";
  } else{
    return "#ffffff" ;
  }
 });


Handlebars.registerHelper('computeAnomaliesString', function(value) {
  if (value == 1){
    return value + " anomaly";
  } else{
    return value + " anomalies" ;
  }
});

Handlebars.registerHelper('getMetricNameFromAlias', function(value) {
  return value.split('::')[1];
});


var SI_PREFIXES = ["", "k", "M", "G", "T", "P", "E"];

Handlebars.registerHelper('abbreviateNumber', function(value, digits) {
   if(digits == undefined){
     digits = 0;
   }
   var num = Number(value);
    var si = [
    { value: 1E18, symbol: "e" },
    { value: 1E15, symbol: "p" },
    { value: 1E12, symbol: "t" },
    { value: 1E9,  symbol: "b" },
    { value: 1E6,  symbol: "m" },
    { value: 1E3,  symbol: "k" }
  ], rx = /\.0+$|(\.[0-9]*[1-9])0+$/, i;
  for (i = 0; i < si.length; i++) {
    if (Math.abs(num) >= si[i].value) {
      return (num / si[i].value).toFixed(digits).replace(rx, "$1") + si[i].symbol;
    }
  }
  return num.toFixed(digits).replace(rx, "$1");
});

/**
 * Helper determining if passed param is an object
 * @param  {Object|Array}  item
 * @return {Boolean}
 */
Handlebars.registerHelper('isObject', function(item) {
  return typeof item === 'object' && !Array.isArray(item);
});

/**
 * Display Human Readable filterNames
 * @param  {String} filter
 * @return {String} Sanizited Filter Name
 */
Handlebars.registerHelper('displayFilterName', function(filter) {
  return filter.split('FilterMap')[0];
});

/**
 * Display Human Readable filters
 * @param  {String} filters
 * @return {String} Sanizited Filter
 */
Handlebars.registerHelper('parseFilters', function(filters) {
  filters = JSON.parse(filters);
  const uiFilters = Object.keys(filters).map((filter) => {
    return `${filter}: ${filters[filter].join(', ')}</br>`
  }).join('');
  return new Handlebars.SafeString(uiFilters);
});


