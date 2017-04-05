
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
  if(doubleValue >= 0){
    return "" + doubleValue.toFixed(1);
  } else {
    return "" + doubleValue.toFixed(1);
  }
});
Handlebars.registerHelper('formatDelta', function (a, b) {
  delta = a - b;
  if(delta >= 0){
    return "+" + delta.toFixed(1);
  } else {
    return "" + delta.toFixed(1);
  }
});

Handlebars.registerHelper('formatNumber', function(num) {
  return num.toString().replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
});

/**
 * Template helper that assigns the correct css class based on the delta
 * @param  {string} value: string value of delta ('2.4%')
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
  return moment(date).tz(tz).format('M-D H');
});

Handlebars.registerHelper('displayMonthDay', function (date) {
  var tz = getTimeZone();
  return moment(date).tz(tz).format('M-D');
});

Handlebars.registerHelper('if_eq', function(a, b, opts) {
  if (a == b) {
      return opts.fn(this);
  } else {
      return opts.inverse(this);
  }
});

Handlebars.registerHelper('if_no_anomalies', function(info, opts) {
  if( info.open == 0 && info.resolved == 0) {
    return opts.fn(this)
  } else{
    return opts.inverse(this);
  }

});

Handlebars.registerHelper('computeColor', function(value) {
 const opacity = Math.abs(value / 25);
 if(value > 0){
   return "rgba(0,0,234," + opacity + ")";
 } else{
   return "rgba(234,0,0,"  + opacity + ")" ;
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

