"use strict";
if(typeof define !== 'function'){
	var define = require('amdefine')(module);
}
define(function(require){
var deep = require("deep");
var mongo = require('mongodb'),
ObjectID = require('bson/lib/bson/objectid').ObjectID,
rqlToMongo = require("./rql-to-mongo");


deep.store.Mongo = deep.compose.Classes(deep.Store, function(protocole, url, collectionName, schema, options){
	if(schema)
		this.schema = schema;
	if(url)
		this.url = url;
	if(collectionName)
		this.collectionName = collectionName;
	if(options)
		deep.utils.up(options, this);
},
{
		url:null,
		collectionName:null,
		init:function(options){
			if(this.initialised)
				return this.initialised.promise();
			console.log("MONGO STORE INIT ");
			options = options || {};
			var url = options.url || this.url;
			if(!url)
				return deep.errors.Store("Mongo failed to init : no url provided !");
			var collectionName = options.collectionName || this.collectionName;
			var self = this;
			var def = deep.Deferred();
			//console.log("MONGO STORE INIT : try connect");
			mongo.connect(url, function(err, db){
				if(err){
					console.error('Failed to connect to mongo database ' + url + ' - error: ' + err.message);
					def.reject(err);
				}
				else
					db.collection(collectionName, function (err, coll){
						if(err){
							console.error("Failed to load mongo database collection : " + url + " : " + collectionName + " error " + err.message);
							def.reject(err);
						}else{
							self.collection = coll;
							console.log("MONGO DB : initialised : ",url, collectionName);
							def.resolve(self);
						}
					});
			});
			this.initialised = def
			return def.promise();
		},
		get: function(id, options){
			//console.log("Mongo : get : ", id, options);//
			options = options || {};
			if(id[0] === "?")
				return this.query(id.substring(1), options);
			var def = deep.Deferred();
			var self = this;
			self.collection.findOne({id: id}, function(err, obj){
				if (err) return def.reject(err);
				if (obj) delete obj._id;
				if(obj === null)
					obj = undefined;
				def.resolve(obj);
			});
			return def.promise()
			.done(function(res){
				//console.log("mongstore (real) get response : ",res);
				if(typeof res === 'undefined' || res === null)
					return deep.errors.NotFound();
				if(res && res.headers && res.status && res.body)
					return deep.errors.Server(res.body, res.status);
			})
			.fail(function(error){
				//console.log("error while calling (get)  Mongoservices - for id : "+id, error);
				return deep.errors.NotFound(error);
			});
		},
		post: function(object, options)
		{
			//console.log("Mongo will do post")
			var deferred = deep.Deferred();
			options = options || {};
			var id = options.id || object.id;
			if (!object.id) object.id = id;
			var self = this;
			var search = {id: id};
			self.collection.findOne(search, function(err, found){
				if (err)
					return deferred.reject(err);
				if (found === null)
				{
					if (!object.id) object.id = ObjectID.createPk().toJSON();
					self.collection.insert(object, function(err, obj){
						if (err)
							return deferred.reject(err);
						obj = obj && obj[0];
						if (obj)
							delete obj._id;
						//console.log("mongstore (real) post response : ",obj);
						deferred.resolve(obj);
					});
				}
				else
					deferred.reject(deep.errors.Store("Mongo store : post failed : "+id + " exists, and can't be overwritten"));
			});
			return deferred.promise()
			.then(function (res){
				if(res && res.headers && res.status && res.body)
					return deep.errors.Server(res.body, res.status);
			},function  (error) {
				//console.log("error while calling Mongoservices : - ", error);
				return deep.errors.Post(error);
			});
		},
		put: function(object, options)
		{
			var deferred = deep.Deferred();
			options = options || {};
			var id = options.id || object.id;
			if (!object.id && !options.query)
				object.id = id;
			var search = {id: id};
			var self = this;
			var schema = this.schema;

			var doUpdate = function(obj){
				if(schema)
				{
					if(schema._deep_ocm_)
						schema = schema("put");
					var report = deep.validate(obj, schema);
					if(!report.valid)
						return deferred.reject(deep.errors.PreconditionFail(report));
				}
				self.collection.update(search, object, {upsert:false, safe:true}, function(err, obj){
					if (err)
						return deferred.reject(err);
					if (obj)
						delete obj._id;
					//console.log("mongstore (real) put response : ", object);
					deferred.resolve(obj);
				});
			};

			if(options.query)
				self.collection.findOne({id: id}, function(err, obj){
					if (err)
						return deferred.reject(err);
					if (obj)
						return deferred.reject(deep.errors.Put("no object found to put with query"));
					deep.utils.replace(obj, options.query, object);
					doUpdate(obj);
				});
			else
				doUpdate(object);
			
			return deferred.promise()
			.then(function (res){
				if(res && res.headers && res.status && res.body)
					return deep.errors.Internal(res.body, res.status);
			},function  (error) {
				//console.log("error while calling Mongoservices : - ", error);
				return deep.errors.Put(error);
			});
		},
		query: function(query, options)
		{
			//console.log("deep.stores.Mongo query : ", query, options);
			options = options || {};

			var headers = (options.response && options.response.headers) || {};

			//headers["Accept-Language"] = options["accept-language"];
			if(this.headers)
				deep.utils.bottom(this.headers, headers);
			if(headers.start || headers.end){
				headers.range = "items=" + headers.start + '-' + headers.end;
			}
			query = query.replace(/\$[1-9]/g, function(t){
				return JSON.stringify(headers.parameters[t.substring(1) - 1]);
			});

			var deferred = deep.Deferred();
			var self = this;
			// compose search conditions
			var x = rqlToMongo.parse(query, options);
			var meta = x[0], search = x[1];

			// range of non-positive length is trivially empty
			//if (options.limit > options.totalCount)
			//	options.limit = options.totalCount;
			if (meta.limit <= 0) {
				var results = [];
				results.totalCount = 0;
				return results;
			}

			// request full recordset length
			// N.B. due to collection.count doesn't respect meta.skip and meta.limit
			// we have to correct returned totalCount manually.
			// totalCount will be the minimum of unlimited query length and the limit itself
			function getCount(arg){
				var def  = deep.Deferred();
				self.collection.count(arg, function(err, count) {
					if(err)
						return def.reject(err);
					def.resolve(count);
				});
				return def.promise();
			}
		
			var totalCountPromise = (meta.totalCount) ?
				getCount(search).done(function(totalCount){
					totalCount -= meta.lastSkip;
					if (totalCount < 0)
						totalCount = 0;
					if (meta.lastLimit < totalCount)
						totalCount = meta.lastLimit;
					// N.B. just like in rql/js-array
					return Math.min(totalCount, typeof meta.totalCount === "number" ? meta.totalCount : Infinity);
				}) : undefined;

			// request filtered recordset
			self.collection.find(search, meta, function(err, cursor){
				if (err)
					return deferred.reject(err);
				cursor.toArray(function(err, results){
					if (err)
						return deferred.reject(err);
					// N.B. results here can be [{$err: 'err-message'}]
					// the only way I see to distinguish from quite valid result [{_id:..., $err: ...}] is to check for absense of _id
					if (results && results[0] && results[0].$err !== undefined && results[0]._id === undefined)
						return deferred.reject(results[0].$err);
					var fields = meta.fields;
					var len = results.length;
					// damn ObjectIDs!
					for (var i = 0; i < len; i++) {
						delete results[i]._id;
					}
					// total count
					deep.when(totalCountPromise)
					.done(function (result){
						results.count = results.length;
						results.start = meta.skip;
						results.end = meta.skip + results.count;
						results.schema = self.schema;
						results.totalCount = result;
						deferred.resolve(results);
					})
					.fail(function (error) {
						deferred.reject(error);
					});
				});
			});
			//console.log("deep.stores.Mongo will do query : ", query);
			return deferred.promise().then(function(results){
				//console.log("deep.stores.Mongo query res : ", results);
				if(typeof results === 'undefined' || results === null)
					return deep.errors.NotFound();
				if(results && results.headers && results.status && results.body)
					return deep.errors.Server(results.body, results.status);
				if(!options.range)
					if(results && results._range_object_)
						return Array.prototype.slice.apply(results.results);
					else
						return Array.prototype.slice.apply(results);
				return deep.when(results.totalCount)
				.done(function (count) {
					//console.log("deep.stores.Mongo range query res : ", results);
					var res = deep.utils.createRangeObject(results.start, results.end-1, count);
					delete results.count;
					delete results.start;
					delete results.end;
					delete results.schema;
					delete results.totalCount;
					res.results = Array.prototype.slice.apply(results);
					res._range_object_ = true;
					return res;
				});
			},function  (error) {
				//console.log("error while calling (query) Mongoservices :  - ", error);
				return deep.errors.Store(error);
			});
		},
		del: function(id, options){
			var deferred = deep.Deferred();
			var search = {id: id};
			this.collection.remove(search, function(err, result){
				if (err) return deferred.reject(err);
				deferred.resolve(true);
			});
			return deferred.promise();
		}
	});

	deep.utils.sheet(deep.store.ObjectSheet, deep.store.Mongo.prototype);

	deep.store.Mongo.create = function(protocole, url, collection, schema, options){
		return new deep.store.Mongo(protocole, url, collection, schema, options);
	};

	return deep.store.Mongo;
});
