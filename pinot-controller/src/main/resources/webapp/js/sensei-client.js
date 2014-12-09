// Is a variable is defined.
function isDefined(object, variable){
	return (typeof(eval(object)[variable]) == 'undefined')? false : true;
};

function fixUrl(origUrl){
    var url="";
    if (origUrl != null){
      if (origUrl.substr(-1) != "/"){
        url = origUrl+"/";
      }
      else{
        url = origUrl;
      }
    }
    return url;
}

// String trim function.
String.prototype.trim = function() { return this.replace(/^\s+|\s+$/g, ''); };

// SenseiFacet(name, expand=true, minHits=1, maxCounts=10, orderBy=HITS)
var SenseiFacet = function () {
	if (arguments.length == 0) return null;

	this.expand = true;
	this.minHits = 1;
	this.maxCounts = 10;
	this.orderBy = this.OrderBy.HITS;

	this.name = arguments[0];
	if (arguments.length > 1)
		this.expand = arguments[1];
	if (arguments.length > 2)
		this.minHits = arguments[2];
	if (arguments.length > 3)
		this.maxCounts = arguments[3];
	if (arguments.length > 4)
		this.orderBy = arguments[4];
};

SenseiFacet.prototype = {
	OrderBy: {
		HITS: "hits",
		VALUE: "val"
	}
};

var SenseiProperty = function (key, val) {
	this.key = key;
	this.val = val;
};

// SenseiSelection(name, values="", excludes="", operation=OR)
var SenseiSelection = function () {
	if (arguments.length == 0) return null;

	this.values = "";
	this.excludes = "";
	this.operation = this.Operation.OR;
	this.properties = [];

	this.name = arguments[0];
	if (arguments.length > 1)
		this.values = arguments[1];
	if (arguments.length > 2)
		this.excludes = arguments[2];
	if (arguments.length > 3)
		this.operation = arguments[3];
};

SenseiSelection.prototype = {
	Operation: {
		OR: "or",
		AND: "and"
	},

	addProperty: function (key, val) {
		for (var i=0; i<this.properties.length; ++i) {
			if (this.properties[i].key == key) {
				this.properties[i].val = val;
				return true;
			}
		}
		this.properties.push(new SenseiProperty(key, val));
		return true;
	},

	removeProperty: function (key) {
		for (var i=0; i<this.properties.length; ++i) {
			if (this.properties[i].key == key) {
				this.properties.splice(i, 1);
				return true;
			}
		}
		return false;
	}
};

// SenseiSort(field, dir=DESC)
var SenseiSort = function () {
	if (arguments.length == 0) return null;

	this.dir = this.DIR.DESC;

	this.field = arguments[0];
	if (arguments.length > 1)
		this.dir = arguments[1];
};

SenseiSort.prototype = {
	DIR: {
		ASC: "asc",
		DESC: "desc"
	}
};

// SenseiClient(query="", offset=0, length=10, explain=false, fetch=false, routeParam, groupBy, maxPerGroup)
var SenseiClient = function () {
	this._facets = [];
	this._selections = [];
	this._sorts = [];
  this._initParams = [];

	this.query = "";
	this.offset = 0;
	this.length = 10;
	this.explain = false;
	this.fetch = false;
  this.fetchTermVectors = [];
	this.routeParam = "";
	this.groupBy = "";
	this.maxPerGroup = 0;
  this.url=null;

	if (arguments.length > 0)
		this.query = arguments[0];
	if (arguments.length > 1)
		this.offset = arguments[1];
	if (arguments.length > 2)
		this.length = arguments[2];
	if (arguments.length > 3)
		this.explain = arguments[3];
	if (arguments.length > 4)
		this.fetch = arguments[4];
	if (arguments.length > 5)
		this.routeParam = arguments[5];
	if (arguments.length > 6)
		this.groupBy = arguments[6];
	if (arguments.length > 7)
		this.maxPerGroup = arguments[7];
};

SenseiClient.prototype = {
	addInitParam: function (initParam) {
		if (!initParam) return false;

		for (var i=0; i<this._initParams.length; ++i) {
			if (initParam.name == this._initParams[i].name) {
				this._initParams.splice(i, 1, initParam);
				return true;
			}
		}
		this._initParams.push(initParam);

		return true;
	},

	removeInitParam: function (name) {
		for (var i=0; i<this._initParams.length; ++i) {
			if (name == this._initParams[i].name) {
				this._initParams.splice(i, 1);
				return true;
			}
		}
		return false;
	},

	clearInitParams: function () {
		this._initParams = [];
	},

    addFacet: function (facet) {
        if (!facet) return false;

        for (var i=0; i<this._facets.length; ++i) {
            if (facet.name == this._facets[i].name) {
                this._facets.splice(i, 1, facet);
                return true;
            }
        }
        this._facets.push(facet);

        return true;
    },

    removeFacet: function (name) {
        for (var i=0; i<this._facets.length; ++i) {
            if (name == this._facets[i].name) {
                this._facets.splice(i, 1);
                return true;
            }
        }
        return false;
    },

    clearFacets: function () {
        this._facets = [];
    },

	addSelection: function (sel) {
		if (!sel) return false;

		for (var i=0; i<this._selections.length; ++i) {
			if (sel == this._selections[i]) {
				return true;
			}
		}
		this._selections.push(sel);
		return true;
	},

	removeSelection: function (sel) {
		for (var i=0; i<this._selections.length; ++i) {
			if (sel == this._selections[i]) {
				this._selections.splice(i, 1);
				return true;
			}
		}
		return false;
	},

	clearSelections: function () {
		this._selections = [];
	},

	addSort: function (sort) {
		if (!sort) return false;

		for (var i=0; i<this._sorts.length; ++i) {
			if (sort.field == this._sorts[i].field) {
				this._sorts.splice(i, 1, sort);
				return true;
			}
		}
		this._sorts.push(sort);

		return true;
	},

	removeSort: function (field) {
		for (var i=0; i<this._sorts.length; ++i) {
			if (field == this._sorts[i].field) {
				this._sorts.splice(i, 1);
				return true;
			}
		}
		return false;
	},

	clearSorts: function () {
		this._sorts = [];
	},

	buildQuery: function () {
		var qs = {
			q: this.query,
			start: this.offset,
			rows: this.length,
			routeparam: this.routeParam,
			groupby: this.groupBy,
			maxpergroup: this.maxPerGroup
		};
		if (this.explain)
			qs['showexplain'] = true;
		if (this.fetch)
			qs['fetchstored'] = true;

        for (var i = 0; i < this._initParams.length; i++) {
            var inputParam = this._initParams[i];
            qs["dyn."+inputParam.name+".type"] = inputParam.type;
            qs["dyn."+inputParam.name+".val"] = inputParam.vals;
        }
    if (this.fetchTermVectors && this.fetchTermVectors.length>0){
      qa['fetchtermvector']=this.fetchTermVectors.join(',');
    }
		for (var i=0; i<this._facets.length; ++i) {
			var facet = this._facets[i];
			qs["facet."+facet.name+".expand"] = facet.expand;
			qs["facet."+facet.name+".minhit"] = facet.minHits;
			qs["facet."+facet.name+".max"] = facet.maxCounts;
			qs["facet."+facet.name+".order"] = facet.orderBy;
		}
		for (var i=0; i<this._selections.length; ++i) {
			var sel = this._selections[i];
			qs["select."+sel.name+".val"] = sel.values;
			qs["select."+sel.name+".not"] = sel.excludes;
			qs["select."+sel.name+".op"] = sel.operation;
			var props = [];
			for (var j=0; j<sel.properties.length; j++) {
				props.push(""+sel.properties[j].key+":"+sel.properties[j].val);
			}
			props = props.join(',');
			if (props != '')
				qs["select."+sel.name+".prop"] = props;
		}
		var sl = [];
		for (var i=0; i<this._sorts.length; ++i) {
			var sort = this._sorts[i];
			if (sort.field == "relevance") {
				sl.push(sort.field);
			}
			else {
				sl.push(sort.field+":"+sort.dir);
			}
		}
		qs["sort"] = sl.join(',');

		return qs;
  },
  
  

	getSysInfo: function (callback) {
    var url = fixUrl(this.url);
		$.getJSON(url+"sensei/sysinfo", null, callback);
	},

	request: function (callback) {
    var url = fixUrl(this.url);
		$.getJSON(url+"sensei", this.buildQuery(), callback);
	}
};


function trim_leading_comments(str)
{
    // very basic. doesn't support /* ... */
    str = str.replace(/^(\s*\/\/[^\n]*\n)+/, '');
    str = str.replace(/^\s+/, '');
    return str;
}


(function($){$.toJSON=function(o)
{if(typeof(JSON)=='object'&&JSON.stringify)
return JSON.stringify(o);var type=typeof(o);if(o===null)
return"null";if(type=="undefined")
return undefined;if(type=="number"||type=="boolean")
return o+"";if(type=="string")
return $.quoteString(o);if(type=='object')
{if(typeof o.toJSON=="function")
return $.toJSON(o.toJSON());if(o.constructor===Date)
{var month=o.getUTCMonth()+1;if(month<10)month='0'+month;var day=o.getUTCDate();if(day<10)day='0'+day;var year=o.getUTCFullYear();var hours=o.getUTCHours();if(hours<10)hours='0'+hours;var minutes=o.getUTCMinutes();if(minutes<10)minutes='0'+minutes;var seconds=o.getUTCSeconds();if(seconds<10)seconds='0'+seconds;var milli=o.getUTCMilliseconds();if(milli<100)milli='0'+milli;if(milli<10)milli='0'+milli;return'"'+year+'-'+month+'-'+day+'T'+
hours+':'+minutes+':'+seconds+'.'+milli+'Z"';}
if(o.constructor===Array)
{var ret=[];for(var i=0;i<o.length;i++)
ret.push($.toJSON(o[i])||"null");return"["+ret.join(",")+"]";}
var pairs=[];for(var k in o){var name;var type=typeof k;if(type=="number")
name='"'+k+'"';else if(type=="string")
name=$.quoteString(k);else
continue;if(typeof o[k]=="function")
continue;var val=$.toJSON(o[k]);pairs.push(name+":"+val);}
return"{"+pairs.join(", ")+"}";}};$.evalJSON=function(src)
{if(typeof(JSON)=='object'&&JSON.parse)
return JSON.parse(src);return eval("("+src+")");};$.secureEvalJSON=function(src)
{if(typeof(JSON)=='object'&&JSON.parse)
return JSON.parse(src);var filtered=src;filtered=filtered.replace(/\\["\\\/bfnrtu]/g,'@');filtered=filtered.replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g,']');filtered=filtered.replace(/(?:^|:|,)(?:\s*\[)+/g,'');if(/^[\],:{}\s]*$/.test(filtered))
return eval("("+src+")");else
throw new SyntaxError("Error parsing JSON, source is not valid.");};$.quoteString=function(string)
{if(_escapeable.test(string))
{return'"'+string.replace(_escapeable,function(a)
{var c=_meta[a];if(typeof c==='string')return c;c=a.charCodeAt();return'\\u00'+Math.floor(c/16).toString(16)+(c%16).toString(16);})+'"';}
return'"'+string+'"';};var _escapeable=/["\\\x00-\x1f\x7f-\x9f]/g;var _meta={'\b':'\\b','\t':'\\t','\n':'\\n','\f':'\\f','\r':'\\r','"':'\\"','\\':'\\\\'};})(jQuery);
