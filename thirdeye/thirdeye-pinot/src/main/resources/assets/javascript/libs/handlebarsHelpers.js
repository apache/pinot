
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
