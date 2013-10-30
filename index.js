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
				self.db = function(){ return db; };

				if(err){
					console.error('Failed to connect to mongo database ' + url + ' - error: ' + err.message);
					return def.reject(err);
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
			this.initialised = def;
			return def.promise();
		},
		get: function(id, options){
			//console.log("Mongo : get : ", id, options);//
			options = options || {};
			if(id[0] === "?" || !id)
				return this.query(id.substring(1), options)
				/*.done(function(s){
					console.log("res from mongo get : ", s);
				});*/
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
				console.log("error while calling (get)  Mongoservices - for id : "+id, error);
				return deep.errors.NotFound(error);
			});
		},
		post: function(object, options)
		{
			//console.log("Mongo will do post")
			var deferred = deep.Deferred();
			options = options || {};
			var id = options.id || object.id;
			if (!object.id)
				object.id = id;
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
				//console.log("mongo.put : search : ", search, " - obj : ", obj);
				self.collection.update(search, obj, {upsert:false, safe:true}, function(err, response){
					if (err)
						return deferred.reject(err);
					if (obj)
						delete response._id;
					//console.log("mongstore (real) put response : ", response);
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
		MAX_QUERY_LIMIT:500,
		query: function(query, options)
		{
			options = options || {};

			var parsingDirectives = {}

			var deferred = deep.Deferred();
			var self = this;

			var noRange = false;
			// add max limit
			if(!options.start && !options.end)
			{
				noRange = true;
				options.start = 0;
				options.end = this.MAX_QUERY_LIMIT;
			}
			options.start = options.start || 0;
			options.end = options.end || 0;
			if(options.end - options.start > this.MAX_QUERY_LIMIT)
				options.end = options.start + this.MAX_QUERY_LIMIT;

			query += "&limit("+((options.end-options.start)+1)+","+options.start+")";

			// compose search conditions
			var x = rqlToMongo.parse(query, parsingDirectives);
			var meta = x[0], search = x[1];

			// range of non-positive length is trivially empty
			//if (options.limit > options.totalCount)
			//	options.limit = options.totalCount;
			if (meta.limit <= 0) {
				var rangeObject = deep.utils.createRangeObject(0, 0, 0);
				rangeObject.results = [];
				rangeObject.count = 0;
				rangeObject.query = query;
				return rangeObject;
			}

			// request full recordset length
			// N.B. due to collection.count doesn't respect meta.skip and meta.limit
			// we have to correct returned totalCount manually.
			// totalCount will be the minimum of unlimited query length and the limit itself
			

			var totalCountPromise = null;
			if(!noRange)
				totalCountPromise = this.count(search);

			// request filtered recordset
			self.collection.find(search, meta, function(err, cursor){
				if (err)
					return deferred.reject(err);
				//console.log("mongo cursor : ", cursor);
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
					if(noRange)
						return deferred.resolve(results);
					deep.when(totalCountPromise)
					.done(function (result){
						var rangeObject = deep.utils.createRangeObject(options.start, Math.max(options.start,options.start+results.length-1), result);
						rangeObject.count = results.length;
						rangeObject.query = query;
						rangeObject.results = results.concat([]);
						deferred.resolve(rangeObject);
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
		},
		range:function(start, end, query)
		{
			return this.query(query || "", { start:start, end:end });
		},
		flush:function(options){
			var def = deep.Deferred();
			var self = this;
			this.init()
			.done(function(success){
				self.db().dropDatabase(function(err, done)
				{
					if(err)
						def.reject(err);
					def.resolve(done || true);
				});
			});
			return def.promise();
		},
		count:function (arg){
			var def  = deep.Deferred();
			this.collection.count(arg, function(err, totalCount) {
				if(err)
					return def.reject(err);
				return def.resolve(totalCount);
			});
			return def.promise();
		}
	});

	deep.utils.sheet(deep.store.ObjectSheet, deep.store.Mongo.prototype);

	//console.log("MONGOSTORE afetr sheet : ",deep.store.Mongo.prototype )

	deep.store.Mongo.create = function(protocole, url, collection, schema, options){
		return new deep.store.Mongo(protocole, url, collection, schema, options);
	};

	deep.coreUnits = deep.coreUnits || [];
    deep.coreUnits.push("js::deep-mongo/units/generic");

	return deep.store.Mongo;
});
