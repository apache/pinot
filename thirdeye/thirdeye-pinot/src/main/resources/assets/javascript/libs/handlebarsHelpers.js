
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
