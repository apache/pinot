
//takes utc date iso format ie. 2016-04-06T07:00:00.000Z, returns date and time in users timezone
Handlebars.registerHelper('displayDate', function (date) {
  var tz = getTimeZone();
  // return moment(date).tz(tz).format('YYYY-MM-DD h a z');
  return moment(date).tz(tz).format('YYYY-MM-DD');
});

Handlebars.registerHelper('displayHour', function (date) {
  var tz = getTimeZone();
  return moment(date).tz(tz).format('h a');
});

Handlebars.registerHelper('displayMonthDayHour', function (date) {
  var tz = getTimeZone();
  return moment(date).tz(tz).format('M-D H');
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
 opacity = Math.abs(value/25);
 if(value > 0){
   return "rgba(0,0,234," + opacity + ")";
 } else{
   return "rgba(234,0,0,"  + opacity + ")" ;
 }
});

//compute the text color so that its visible based on background
Handlebars.registerHelper('computeTextColor', function(value) {
  opacity = Math.abs(value/25);
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


