/* 
 * Original script by Josh Fraser (http://www.onlineaspect.com)
 * Continued and maintained by Jon Nylander at https://bitbucket.org/pellepim/jstimezonedetect
 *
 * Provided under the Do Whatever You Want With This Code License.
 */

/**
 * Namespace to hold all the code for timezone detection.
 */
var jstz = function () {
  var HEMISPHERE_SOUTH = 's',
		retval = {
			timezone_name : '',
			uses_dst : '',
			utc_offset : 0,
			utc_name : '',
			hemisphere : ''
		};
	/** 
	 * Gets the offset in minutes from UTC for a certain date.
	 * @param {Date} date
	 * @returns {Number}
	 */
	function get_date_offset(date) {
		var offset = -date.getTimezoneOffset();
		return (offset !== null ? offset : 0);
	};

	function get_january_offset() {
		return get_date_offset(new Date(2010, 0, 1, 0, 0, 0, 0));
	};

	function get_june_offset() {
		return get_date_offset(new Date(2010, 5, 1, 0, 0, 0, 0));
	};

	/**
	 * Checks whether a given date is in daylight savings time.
	 * If the date supplied is after june, we assume that we're checking
	 * for southern hemisphere DST.
	 * @param {Date} date
	 * @returns {Boolean}
	 */
	function date_is_dst(date) {
		var base_offset = ((date.getMonth() > 5 ? get_june_offset() : get_january_offset())),
			date_offset = get_date_offset(date); 
		return (base_offset - date_offset) !== 0;
	};

	function set_utc_name(){
		retval.utc_name = '';
		var hour, min, sign;
		min = parseInt(retval.utc_offset % 60);
		if( min != 0 ) return;
		hour = parseInt(retval.utc_offset / 60);
		if( retval.dst ) hour += 1;
		sign = retval.utc_offset<0 ? '+' : '-';
		retval.utc_name = 'Etc/GMT' + sign + Math.abs(hour);
	}
	/**
	 * This function does some basic calculations to create information about 
	 * the user's timezone.
	 * 
	 * Returns a key that can be used to do lookups in jstz.olson.timezones.
	 * 
	 * @returns {String}  
	 */
	function lookup_olson_table() {
		var tmp, key;
		key = '' + retval.utc_offset;
		if( jstz.olson.timezones[key] ) tmp = jstz.olson.timezones[key];
		key += retval.uses_dst ? ',1' : ',0';
		if( jstz.olson.timezones[key] ) tmp = jstz.olson.timezones[key];
		//really nasty hack for "Mid-Atantic" windows timezone
		if(key == "-120,1"){
			var actual_offset = get_date_offset(new Date()),
				january_offset = get_january_offset();
			if(actual_offset != january_offset){
				retval.timezone_name = jstz.olson.timezones['-60,0'];
				retval.hemisphere = 'south';
			}
		}
		key += retval.hemisphere == 'south'? 's' : '';
		if( jstz.olson.timezones[key] ) tmp = jstz.olson.timezones[key];
		return tmp;
	};

	/**
	 * Checks if a timezone has possible ambiguities. I.e timezones that are similar.
	 * 
	 * If the preliminary scan determines that we're in America/Denver. We double check
	 * here that we're really there and not in America/Mazatlan.
	 * 
	 * This is done by checking known dates for when daylight savings start for different
	 * timezones.
	 */
	function ambiguity_check() {
		var ambiguity_list = jstz.olson.ambiguity_list[retval.timezone_name],
			length = ambiguity_list.length,
			tz = ambiguity_list[0],
			i;
		for (i = 0; i < length; i++) {
			tz = ambiguity_list[i];
			if (date_is_dst(jstz.olson.dst_start_dates[tz])) {
				retval.timezone_name = tz;
				return;
			} 
		}
	};

	/**
	 * Main function
	 */
	function determine() {
		var january_offset = get_january_offset(), 
			june_offset = get_june_offset(),
			diff = january_offset - june_offset;
		if (diff < 0) retval.utc_offset = january_offset;
		if (diff > 0) retval.utc_offset = june_offset;
		retval.uses_dst = diff == 0 ? false : true;
		retval.utc_offset = january_offset;
		var tmp = lookup_olson_table();
		retval.timezone_name = tmp[0];
		if( tmp[1]==='s' ) retval.hemisphere = 'south';
		if( tmp[1]==='n' ) retval.hemisphere = 'north';
		//handle ambiguous time zones
		if (jstz.olson.is_ambiguous(retval.timezone_name)) {
			ambiguity_check();
		}
		set_utc_name();
		return retval;
	};

	return determine();
};

jstz.olson = {
	/**
	 * The keys in this dictionary are comma separated as such:
	 * 
	 * First the offset compared to UTC time in minutes.
	 * Then a DST flag which is 0 if DST is disabled or 1 if is enabled
	 * Thirdly an optional 's' signifies that the timezone is in the southern hemisphere
	 */
	timezones : {
		'-720'     : ['Etc/GMT+12',''],
		'-660,0'   : ['Pacific/Pago_Pago','n'],
		'-600,1'   : ['America/Adak','n'],
		'-660,1,s' : ['Pacific/Apia','s'],
		'-600,0'   : ['Pacific/Honolulu','n'],
		'-570'     : ['Pacific/Marquesas','n'],
		'-540,0'   : ['Pacific/Gambier',''],
		'-540,1'   : ['America/Anchorage','n'],
		'-480,1'   : ['America/Los_Angeles','n'],
		'-480,0'   : ['Pacific/Pitcairn','n'],
		'-420,0'   : ['America/Phoenix','n'],
		'-420,1'   : ['America/Denver','n'],
		'-360,0'   : ['America/Guatemala','n'],
		'-360,1'   : ['America/Chicago','n'],
		'-360,1,s' : ['Pacific/Easter','s'],
		'-300,0'   : ['America/Bogota','n'],
		'-300,1'   : ['America/New_York','n'],
		'-270'     : ['America/Caracas','n'],
		'-240,1'   : ['America/Halifax','n'],
		'-240,0'   : ['America/Santo_Domingo',''],
		'-240,1,s' : ['America/Asuncion','s'],
		'-210'     : ['America/St_Johns','n'],
		'-180,1'   : ['America/Godthab','n'],
		'-180,0'   : ['America/Argentina/Buenos_Aires','s'],
		'-180,1,s' : ['America/Montevideo','s'],
		'-120,0'   : ['America/Noronha','s'],
		'-120,1'   : ['Atlantic/South_Georgia','s'],
		'-60,1'    : ['Atlantic/Azores',''],
		'-60,0'    : ['Atlantic/Cape_Verde','s'],
		'0,0'      : ['Etc/UTC',''],
		'0,1'      : ['Europe/London','n'],
		'60,0'     : ['Africa/Lagos','n'],
		'60,1'     : ['Europe/Berlin','n'],
		'60,1,s'   : ['Africa/Windhoek','s'],
		'120,1'    : ['Asia/Beirut','n'],
		'120,0'    : ['Africa/Johannesburg','n'],
		'180,1'    : ['Europe/Moscow','n'],
		'180,0'    : ['Asia/Baghdad','s'],
		'210,1'    : ['Asia/Tehran','n'],
		'240,0'    : ['Asia/Dubai','n'],
		'240,1'    : ['Asia/Yerevan','n'],
		'240,1,s'  : ['Etc/UTC+4','s'],
		'270'      : ['Asia/Kabul','n'],
		'300,1'    : ['Asia/Yekaterinburg','n'],
		'300,0'    : ['Asia/Karachi','n'],
		'330'      : ['Asia/Kolkata','n'],
		'345'      : ['Asia/Kathmandu','n'],
		'360,0'    : ['Asia/Dhaka','n'],
		'360,1'    : ['Asia/Omsk','n'],
		'390,0'    : ['Asia/Rangoon','n'],
		'420,1'    : ['Asia/Krasnoyarsk','n'],
		'420,0'    : ['Asia/Jakarta','n'],
		'480,0'    : ['Asia/Shanghai','n'],
		'480,1'    : ['Asia/Irkutsk','n'],
		'480,1,s'  : ['Australia/Perth','s'],
		'525,0'    : ['Australia/Eucla','n'],
		'525,1'    : ['Australia/Eucla','s'],
		'540,1'    : ['Asia/Yakutsk','n'],
		'540,0'    : ['Asia/Tokyo','n'],
		'570,0'    : ['Australia/Darwin','n'],
		'570,1'    : ['Australia/Adelaide','s'],
		'600,0'    : ['Australia/Brisbane','n'],
		'600,1'    : ['Asia/Vladivostok','n'],
		'600,1,s'  : ['Australia/Sydney','s'],
		'630'      : ['Australia/Lord_Howe','s'],
		'660,1'    : ['Asia/Kamchatka','n'],
		'660,0'    : ['Pacific/Noumea','n'],
		'690'      : ['Pacific/Norfolk','n'],
		'720,1'    : ['Etc/GMT+12','n'],
		'720,1,s'  : ['Pacific/Auckland','s'],
		'720,0'    : ['Pacific/Tarawa','n'],
		'765'      : ['Pacific/Chatham','s'],
		'780,0'    : ['Pacific/Tongatapu','n'],
		'780,1,s'  : ['Pacific/Pago_Pago','s'],
		'840,0'    : ['Pacific/Kiritimati','n']
	},
	/**
	 * Checks if it is possible that the timezone is ambiguous.
	 */
	is_ambiguous : function (timezone_name) {
		return typeof (this.ambiguity_list[timezone_name]) !== 'undefined';
	},
	/**
	 * The keys in this object are timezones that we know may be ambiguous after
	 * a preliminary scan.
	 * 
	 * The array of timezones to compare must be in the order that daylight savings
	 * starts for the regions.
	 */
	ambiguity_list : {
		'America/Denver' : ['America/Denver', 'America/Mazatlan'],
		'America/Chicago' : ['America/Chicago', 'America/Mexico_City'],
		'America/Asuncion' : ['Atlantic/Stanley', 'America/Asuncion', 'America/Santiago', 'America/Campo_Grande'],
		'America/Montevideo' : ['America/Montevideo', 'America/Sao_Paulo'],
		'Asia/Beirut' : ['Asia/Gaza', 'Asia/Beirut', 'Europe/Minsk', 'Europe/Helsinki', 'Europe/Istanbul', 'Asia/Damascus', 'Asia/Jerusalem', 'Africa/Cairo'],
		'Asia/Yerevan' : ['Asia/Yerevan', 'Asia/Baku'],
		'Pacific/Auckland' : ['Pacific/Auckland', 'Pacific/Fiji'],
		'America/Los_Angeles' : ['America/Los_Angeles', 'America/Santa_Isabel'],
		'America/New_York' : ['America/Havana', 'America/New_York'],
		'America/Halifax' : ['America/Goose_Bay', 'America/Halifax'],
		'America/Godthab' : ['America/Miquelon', 'America/Godthab']
	},
	/**
	 * Contains information on when DST starts for different timezones.
	 * Each value is a date denoting when daylight savings starts for that timezone.
	 */
	dst_start_dates : {
		'America/Denver' : new Date(2011, 2, 13, 3, 0, 0, 0),
		'America/Mazatlan' : new Date(2011, 3, 3, 3, 0, 0, 0),
		'America/Chicago' : new Date(2011, 2, 13, 3, 0, 0, 0),
		'America/Mexico_City' : new Date(2011, 3, 3, 3, 0, 0, 0),
		'Atlantic/Stanley' : new Date(2011, 8, 4, 7, 0, 0, 0),
		'America/Asuncion' : new Date(2011, 9, 2, 3, 0, 0, 0),
		'America/Santiago' : new Date(2011, 9, 9, 3, 0, 0, 0),
		'America/Campo_Grande' : new Date(2011, 9, 16, 5, 0, 0, 0),
		'America/Montevideo' : new Date(2011, 9, 2, 3, 0, 0, 0),
		'America/Sao_Paulo' : new Date(2011, 9, 16, 5, 0, 0, 0),
		'America/Los_Angeles' : new Date(2011, 2, 13, 8, 0, 0, 0),
		'America/Santa_Isabel' : new Date(2011, 3, 5, 8, 0, 0, 0),
		'America/Havana' : new Date(2011, 2, 13, 2, 0, 0, 0),
		'America/New_York' : new Date(2011, 2, 13, 7, 0, 0, 0),
		'Asia/Gaza' : new Date(2011, 2, 26, 23, 0, 0, 0),
		'Asia/Beirut' : new Date(2011, 2, 27, 1, 0, 0, 0),
		'Europe/Minsk' : new Date(2011, 2, 27, 3, 0, 0, 0),
		'Europe/Helsinki' : new Date(2011, 2, 27, 4, 0, 0, 0),
		'Europe/Istanbul' : new Date(2011, 2, 28, 5, 0, 0, 0),
		'Asia/Damascus' : new Date(2011, 3, 1, 2, 0, 0, 0),
		'Asia/Jerusalem' : new Date(2011, 3, 1, 6, 0, 0, 0),
		'Africa/Cairo' : new Date(2010, 3, 30, 4, 0, 0, 0),
		'Asia/Yerevan' : new Date(2011, 2, 27, 4, 0, 0, 0),
		'Asia/Baku'    : new Date(2011, 2, 27, 8, 0, 0, 0),
		'Pacific/Auckland' : new Date(2011, 8, 26, 7, 0, 0, 0),
		'Pacific/Fiji' : new Date(2010, 11, 29, 23, 0, 0, 0),
		'America/Halifax' : new Date(2011, 2, 13, 6, 0, 0, 0),
		'America/Goose_Bay' : new Date(2011, 2, 13, 2, 1, 0, 0),
		'America/Miquelon' : new Date(2011, 2, 13, 5, 0, 0, 0),
		'America/Godthab' : new Date(2011, 2, 27, 1, 0, 0, 0)
	}
};
