"use strict";
if(typeof define !== 'function'){
	var define = require('amdefine')(module);
}
define(function(require){
var deep = require("deepjs");
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
				return deep.when.immediate(this);
			//console.time("mongo.init");
			this.initialised = true;

			//console.log("MONGO STORE INIT ");
			options = options || {};
			var url = options.url || this.url;
			if(!url)
				return deep.when(deep.errors.Store("Mongo failed to init : no url provided !"));
			var collectionName = options.collectionName || this.collectionName;
			var self = this;
			//console.log("MONGO STORE INIT : try connect");
			return deep.wrapNodeAsynch(mongo, "connect", [url])
			.done(function(db){
				db = db[0];
				self.db = function(){ return db; };
				return deep.wrapNodeAsynch(db, "collection", [collectionName]);
			})
			.done(function(coll){
				coll = coll[0];
				self.collection = coll;
				return self.ensureIndex({ id: 1 }, { unique: true });
			})
			.done(function(){
				console.log("MONGO DB : initialised : ",url, collectionName);
			})
			.fail(function(err){
				console.error('Failed to connect to mongo database ' + url + ' - error: ' + err.message);
			});
		},
		get: function(id, options){
			//console.log("Mongo : get : ", id, options);//
			options = options || {};
			if(id == 'schema')
			{
				//console.log("deep-mongo : get schema : ", this.schema || {});
				if(this.schema && this.schema._deep_ocm_)
					return deep.when(this.schema("get"));
				return this.schema || {};
			}
			if(!id)
				id = "";
			if(!id || id[0] === "?")
				return this.query(id.substring(1), options);
			var self = this;
			var schema = this.schema;
			var search = { id:id };
			if(schema)
			{
				if(schema._deep_ocm_)
					schema = schema("get");
				if(schema.ownerRestriction)
				{
                    if(deep.context.session && deep.context.session.remoteUser)
						search.userID = deep.context.session.remoteUser.id;
                    else
                        return deep.when(deep.errors.Owner());
				}
			}
			return deep.wrapNodeAsynch(self.collection, "findOne", [search])
			.done(function(obj){
				obj = obj[0];
				if (obj)
					delete obj._id;
				else
					return deep.errors.NotFound("maybe ownership restriction");
				return obj;
			});
		},
		post: function(object, options)
		{
			//console.log("Mongo will do post")
			//var deferred = deep.Deferred();
			options = options || {};
			var id = options.id || object.id;
			if (!id)
				id = object.id = options.id = ObjectID.createPk().toJSON();
			var self = this;
			var search = {id: id};
			var schema = this.schema;
			if(schema)
			{
				if(schema._deep_ocm_)
					schema = schema("post");
				if(schema.ownerRestriction)
				{
                    if(deep.context.session && deep.context.session.remoteUser)
						object.userID = deep.context.session.remoteUser.id;
                    else
                        return deep.when(deep.errors.Owner("you need to be loged in before posting on this ressource"));
				}
			}
			var context = deep.context;
			return deep.wrapNodeAsynch(self.collection,"insert", [object])
			.fail(function(err){
				if(err.code == 11000)
					return deep.errors.Conflict();
			})
			.done(function(obj){
				if(obj && obj[0])
					obj = obj[0][0];
				if (obj)
					delete obj._id;
				else
					return deep.errors.NotFound();
				//console.log("Mongo.post : result from asynch wrap : ", obj);
				return obj;
			});
		},
		put: function(object, options)
		{
			options = options || {};
			var id = options.id || object.id;
			if(!id)
				return deep.when(deep.errors.Put("need id on put!"));
			if (!object.id && !options.query)
				object.id = id;
			var search = { id: id };
			var self = this;
			var schema = this.schema;
			//var context = deep.context;

			return deep.wrapNodeAsynch(self.collection, "findOne", [search])
			.done(function(obj){
				obj = obj[0];
				if (!obj)
					return deep.errors.NotFound("no object found to update");

				if(schema)
				{
					if(schema._deep_ocm_)
						schema = schema("put");
					if(schema.ownerRestriction)
					{
						//console.log("checking owner on put : ", deep.context, obj)
	                    if(deep.context.session && deep.context.session.remoteUser)
						{
							if(deep.context.session.remoteUser.id != obj.userID)
	                        	return deep.errors.Owner();
						}
	                    else
	                        return deep.errors.Owner();
					}
				}

				var old = deep.utils.copy(obj);
				if(options.query)
					deep.utils.replace(obj, options.query, object);
				else
					obj = object;
				if(!obj.id)
					obj.id = id;

				if(schema)
				{
					var check = self.checkReadOnly(old, obj, options);
					if(check instanceof Error)
						return check;
					var report = deep.validate(obj, schema);
					if(!report.valid)
						return deep.errors.PreconditionFail(report);
				}
				return deep.wrapNodeAsynch(self.collection, "findAndModify",[search, null /* sort */, obj, {upsert:false, "new":true}])
				.done(function(response){
					response = response[0];
					if (response)
						delete response._id;
					else
						return deep.errors.NotFound();
					//console.log("mongstore (real) put response : ", response);
					return response;
				});
			});
		},
		patch: function(object, options)
		{
			//console.log("checking context on patch : ", deep.context)
			options = options || {};
			var id = options.id || object.id;
			if(!id)
				return deep.when(deep.errors.Patch("need id on put!"));
			if (!object.id && !options.query)
				object.id = id;
			var search = {id: id};
			var self = this;
			var schema = this.schema;
			var context = deep.context;

			return deep.wrapNodeAsynch(self.collection, "findOne", [search])
			.done(function(obj){
				obj = obj[0];
				if (!obj)
					return deep.errors.NotFound("no object found for patching");

				if(schema)
				{
					if(schema._deep_ocm_)
						schema = schema("patch");
					if(schema.ownerRestriction)
					{
						//console.log("checking owner on patch : ", deep.context, obj)
	                    if(deep.context.session && deep.context.session.remoteUser)
						{
							if(deep.context.session.remoteUser.id != obj.userID)
	                        	return deep.errors.Owner();
						}
	                    else
	                        return deep.errors.Owner();
					}
				}

				var old = deep.utils.copy(obj);
				if(options.query)
				{
					deep.query(obj, options.query, { resultType:"full", allowStraightQueries:false })
					.forEach(function(entry){
						entry.value = deep.utils.up(object, entry.value);
						if(entry.ancestor)
							entry.ancestor.value[entry.key] = entry.value;
					});
					delete options.query;
				}
				else
					deep.utils.up(object, obj);

				if(schema)
				{
					var check = self.checkReadOnly(old, obj, options);
					//console.log('patch readonly check : ', check)
					if(check instanceof Error)
						return check;
					var report = deep.validate(obj, schema);
					if(!report.valid)
						return deep.errors.PreconditionFail(report);
				}
				//console.log("will patch : ", obj, " - old : ", old)
				//delete obj._id;
				return deep.wrapNodeAsynch(self.collection, "findAndModify", [search, null /* sort */, obj, {upsert:false, "new":true}])
				.done(function(response){
					response = response[0];
					if (response)
						delete response._id;
					else
						return deep.errors.NotFound();
					//console.log("mongstore (real) put response : ", response);
					return response;
				});
			});
		},
		MAX_QUERY_LIMIT:500,
		query: function(query, options)
		{
			options = options || {};
			if(!query)
				query = "";

			var parsingDirectives = {};

			var deferred = deep.Deferred();
			var self = this;

			var noRange = false;
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

			if(query[0] == "?")
				query = query.substring(1);
			// compose search conditions
			var x = rqlToMongo.parse(query, parsingDirectives);
			var meta = x[0], search = x[1];
			query = "?"+query;

			if (meta.limit <= 0) {
				var rangeObject = deep.utils.createRangeObject(0, 0, 0, 0, [], query);
				rangeObject.results = [];
				rangeObject.count = 0;
				rangeObject.query = query;
				return rangeObject;
			}

			var totalCountPromise = null;
			if(!noRange)
				totalCountPromise = this.count(search);

			var schema = this.schema;
			if(schema)
			{
				if(schema._deep_ocm_)
					schema = schema("get");
				if(schema.ownerRestriction)
				{
                    if(deep.context.session && deep.context.session.remoteUser)
						search.userID = deep.context.session.remoteUser.id;
                    else
                        return deep.when(deep.errors.Owner());
				}
			}
			var context = deep.context;
			deep.wrapNodeAsynch(self.collection, "find", [search, meta])
			.done(function(cursor){
				cursor = cursor[0];
				cursor.toArray(function(err, results){
					if (err)
						return deferred.reject(err);
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
						//deep.context = context;
						var rangeObject = deep.utils.createRangeObject(options.start,
							Math.max(options.start,options.start+results.length-1),
							result,
							results.length,
							results,
							query
						);
						deferred.resolve(rangeObject);
					})
					.fail(function (error) {
						deferred.reject(error);
					});
				});
			})
			.fail(function(e){
				deferred.reject(e);
			});
			//________________________
			//console.log("deep.stores.Mongo will do query : ", query);
			return deferred.promise();
		},
		del: function(id, options){
			var search = {id: id};
			var schema = this.schema;
			if(schema)
			{
				if(schema._deep_ocm_)
					schema = schema("put");
				if(schema.ownerRestriction)
				{
                    if(deep.context.session && deep.context.session.remoteUser)
						search.userID = deep.context.session.remoteUser.id;
                    else
                        return deep.when(deep.errors.Owner());
				}
			}
			var context = deep.context;
			return deep.wrapNodeAsynch(this.collection, "remove", [search, { safe:true }])
			.done(function(s){
				return s[0] === 1;
			});
		},
		range:function(start, end, query)
		{
			return this.query(query || "", { start:start, end:end });
		},
		flush:function(options){
			var self = this;
			return this.init()
			.done(function(){
				return deep.wrapNodeAsynch(self.collection, "drop", [])
				.done(function(done){
					return self.ensureIndex({ id: 1 }, { unique: true });
				});
			})
			.fail(function(e){
				console.log("flush error : ", e);
			});
		},
		indexes:function(){
			var self = this;
			return this.init()
			.done(function(success){
				return deep.wrapNodeAsynch(self.collection, "indexes", [])
				.done(function(indexes){
					return indexes[0];
				});
			});
		},
		reIndex:function(){
			var self = this;
			return this.init()
			.done(function(success){
				return deep.wrapNodeAsynch(self.collection, "reIndex", [])
				.done(function(indexes){
					return indexes[0];
				});
			});
		},
		ensureIndex:function (properties, options) {
			var self = this;
			return this.init()
			.done(function(success){
				return deep.wrapNodeAsynch(self.collection, "ensureIndex", [properties, options]);
			})
			.done(function(indexes){
				return indexes[0];
			});
		},
		count:function (arg){
			return deep.wrapNodeAsynch(this.collection, "count", [arg])
			.done(function(totalCount){
				//console.log("mongo count res : ", totalCount)
				return totalCount[0];
			});
		}
	});

	deep.utils.sheet(deep.store.ObjectSheet, deep.store.Mongo.prototype);
	deep.store.Mongo.create = function(protocole, url, collection, schema, options){
		return new deep.store.Mongo(protocole, url, collection, schema, options);
	};

	deep.coreUnits = deep.coreUnits || [];
    deep.coreUnits.push("js::deep-mongo/units/generic");

	return deep.store.Mongo;
});
