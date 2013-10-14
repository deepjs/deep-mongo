/**
 * A walker that take RQL and provide Mongo formatted query.
 * Copied/Pasted from persvr/perstore/store/mongo  (https://github.com/persvr/perstore)
 */
"use strict";
if(typeof define !== 'function'){
	var define = require('amdefine')(module);
}
define(function(require){
	var RQ = require("rql/parser");
	return {
		parse : function(query, directives){
			// parse string to parsed terms
			if(typeof query === "string"){
				// handle $-parameters
				// TODO: consider security issues
				//// N.B. considered, treated as evil, bump
				//throw new URIError("Sorry, we don't allow raw querystrings. Please, provide the parsed terms instead");
				if (directives && directives.parameters) {
					query = query.replace(/\$[1-9]/g, function(param){
						return directives.parameters[param.substring(1) - 1];
					});
				}
				// poorman regexp? *foo, bar*
				/***v = (v.charAt(0) != '*') ? '^' + v : v.substring(1);
				v = (v.slice(-1) != '*') ? v + '$' : v.substring(0, v.length-1);***/
				query = RQ.parseQuery(query);
			}
			var options = {
				skip: 0,
				limit: +Infinity,
				lastSkip: 0,
				lastLimit: +Infinity
			};
			var search = {};
//			var needBulkFetch = directives && directives.postprocess; // whether to fetch whole dataset to process it here
//if (!needBulkFetch) {

			function walk(name, terms) {
				// valid funcs
				var valid_funcs = ['lt','lte','gt','gte','ne','in','nin','not','mod','all','size','exists','type','elemMatch'];
				// funcs which definitely require array arguments
				var requires_array = ['in','nin','all','mod'];
				// funcs acting as operators
				var valid_operators = ['or', 'and'];//, 'xor'];
				// compiled search conditions
				var search = {};
				// iterate over terms
				terms.forEach(function(term){
					var func = term.name;
					var args = term.args;
					// ignore bad terms
					// N.B. this filters quirky terms such as for ?or(1,2) -- term here is a plain value
					if (!func || !args) return;
					//dir(['W:', func, args]);
					// process well-known functions
					// http://www.mongodb.org/display/DOCS/Querying
					if (func == 'sort' && args.length > 0) {
						options.sort = args.map(function(sortAttribute){
							var firstChar = sortAttribute.charAt(0);
							var orderDir = 'ascending';
							if (firstChar == '-' || firstChar == '+') {
								if (firstChar == '-') {
									orderDir = 'descending';
								}
								sortAttribute = sortAttribute.substring(1);
							}
							return [sortAttribute, orderDir];
						});
					} else if (func == 'select') {
						options.fields = args;
					} else if (func == 'values') {
						options.unhash = true;
						options.fields = args;
					// N.B. mongo has $slice but so far we don't allow it
					/*} else if (func == 'slice') {
						//options[args.shift()] = {'$slice': args.length > 1 ? args : args[0]};*/
					} else if (func == 'limit') {
						// we calculate limit(s) combination
						options.lastSkip = options.skip;
						options.lastLimit = options.limit;
						// TODO: validate args, negative args
						var l = args[0] || Infinity, s = args[1] || 0;
						// N.B: so far the last seen limit() contains Infinity
						options.totalCount = args[2];
						if (l <= 0) l = 0;
						if (s > 0) options.skip += s, options.limit -= s;
						if (l < options.limit) options.limit = l;
//dir('LIMIT', options);
					// grouping
					} else if (func == 'group') {
						// TODO:
					// nested terms? -> recurse
					} else if (args[0] && typeof args[0] === 'object') {
						if (valid_operators.indexOf(func) > -1)
							search['$'+func] = walk(func, args);
						// N.B. here we encountered a custom function
						// ...
					// structured query syntax
					// http://www.mongodb.org/display/DOCS/Advanced+Queries
					} else {
						//dir(['F:', func, args]);
						// mongo specialty
						if (func == 'le') func = 'lte';
						else if (func == 'ge') func = 'gte';
						// the args[0] is the name of the property
						var key = args.shift();
						// the rest args are parameters to func()
						if (requires_array.indexOf(func) >= 0) {
							args = args[0];
						} else {
							// FIXME: do we really need to .join()?!
							args = args.length == 1 ? args[0] : args.join();
						}
						// regexps:
						if (typeof args === 'string' && args.indexOf('re:') === 0)
							args = new RegExp(args.substr(3), 'i');
						// regexp inequality means negation of equality
						if (func == 'ne' && args instanceof RegExp) {
							func = 'not';
						}
						// TODO: contains() can be used as poorman regexp
						// E.g. contains(prop,a,bb,ccc) means prop.indexOf('a') >= 0 || prop.indexOf('bb') >= 0 || prop.indexOf('ccc') >= 0
						//if (func == 'contains') {
						//	// ...
						//}
						// valid functions are prepended with $
						if (valid_funcs.indexOf(func) > -1) {
							func = '$'+func;
						}
						// $or requires an array of conditions
						// N.B. $or is said available for mongodb >= 1.5.1
						if (name == 'or') {
							if (!(search instanceof Array))
								search = [];
							var x = {};
							x[func == 'eq' ? key : func] = args;
							search.push(x);
						// other functions pack conditions into object
						} else {
							// several conditions on the same property is merged into one object condition
							if (search[key] === undefined)
								search[key] = {};
							if (search[key] instanceof Object && !(search[key] instanceof Array))
								search[key][func] = args;
							// equality cancels all other conditions
							if (func == 'eq')
								search[key] = args;
						}
					}
				// TODO: add support for query expressions as Javascript
				});
				return search;
			}
			//dir(['Q:',query]);
			search = walk(query.name, query.args);
			//dir(['S:',search]);
			return [options, search];
		}
	};
});



