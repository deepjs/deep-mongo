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
				return this.initialised.promise();
			//console.time("mongo.init");

			//console.log("MONGO STORE INIT ");
			options = options || {};
			var url = options.url || this.url;
			if(!url)
				return deep.errors.Store("Mongo failed to init : no url provided !");
			var collectionName = options.collectionName || this.collectionName;
			var self = this;
			var def = deep.Deferred();
			var context = deep.context;
			//console.log("MONGO STORE INIT : try connect");
			mongo.connect(url, function(err, db){
				self.db = function(){ return db; };
				deep.context = context;
				if(err){
					console.error('Failed to connect to mongo database ' + url + ' - error: ' + err.message);
					return def.reject(err);
				}
				else
					db.collection(collectionName, function (err, coll){
						//console.timeEnd("mongo.init");
						deep.context = context;

						if(err){
							console.error("Failed to load mongo database collection : " + url + " : " + collectionName + " error " + err.message);
							def.reject(err);
						}else{
							coll.ensureIndex( { id: 1 }, { unique: true }, function(err, res){
								deep.context = context;
								if(err){
									console.error("Failed to ensuring index on mongo database collection : " + url + " : " + collectionName + " error " + err.message);
									return def.reject(err);
								}
								self.collection = coll;
								console.log("MONGO DB : initialised : ",url, collectionName);
								def.resolve(self);
							});
							
						}

					});
			});

			this.initialised = def;
			return def.promise();
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
			var def = deep.Deferred();
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
			self.collection.findOne(search, function(err, obj){
				if (err) return def.reject(err);
				if (obj) delete obj._id;
				else
					return def.reject(deep.errors.NotFound("maybe ownership restriction"));
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
			self.collection.insert(object, function(err, obj){
				deep.context = context;
				if (err)
				{
					//console.log("mongo post error : ", err);
					if(err.code == 11000)
						return deferred.reject(deep.errors.Conflict());
					return deferred.reject(err);
				}
				obj = obj && obj[0];
				if (obj)
					delete obj._id;
				//console.log("mongstore (real) post response : ",obj);
				deferred.resolve(obj);
			});

			return deferred.promise()
			.done(function (res){
				if(res && res.headers && res.status && res.body)
					return deep.errors.Server(res.body, res.status);
			});
		},
		put: function(object, options)
		{
			var deferred = deep.Deferred();
			options = options || {};
			var id = options.id || object.id;
			if(!id)
				return deep.when(deep.errors.Put("need id on put!"));
			if (!object.id && !options.query)
				object.id = id;
			var search = {id: id};
			var self = this;
			var schema = this.schema;
			var context = deep.context;

			self.collection.findOne(search, function(err, obj){		// ownership check here
				deep.context = context;
				if (err)
					return deferred.reject(err);
				if (!obj)
					return deferred.reject(deep.errors.NotFound("no object found to update"));

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
	                        	return deferred.reject(deep.errors.Owner());
						}
	                    else
	                        return deferred.reject(deep.errors.Owner());
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
						return deferred.reject(check);
					var report = deep.validate(obj, schema);
					if(!report.valid)
						return deferred.reject(deep.errors.PreconditionFail(report));
				}
				self.collection.findAndModify(search, null /* sort */, obj, {upsert:false, "new":true}, function(err, response){
					deep.context = context;
					if (err)
						return deferred.reject(err);
					if (response)
						delete response._id;
					else
						return deferred.reject(deep.errors.NotFound());
					//console.log("mongstore (real) put response : ", response);
					deferred.resolve(response);
				});
			});

			
			return deferred.promise()
			.done(function (res){
				if(res && res.headers && res.status && res.body)
					return deep.errors.Internal(res.body, res.status);
			});
		},
		patch: function(object, options)
		{
			//console.log("checking context on patch : ", deep.context)
			var deferred = deep.Deferred();
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


			self.collection.findOne(search, function(err, obj){		// ownership check here
				deep.context = context;
				if (err)
					return deferred.reject(err);
				if (!obj)
					return deferred.reject(deep.errors.NotFound("no object found for patching"));

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
	                        	return deferred.reject(deep.errors.Owner());
						}
	                    else
	                        return deferred.reject(deep.errors.Owner());
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
						return deferred.reject(check);
					var report = deep.validate(obj, schema);
					if(!report.valid)
						return deferred.reject(deep.errors.PreconditionFail(report));
				}
				self.collection.findAndModify(search, null /* sort */, obj, {upsert:false, "new":true}, function(err, response){
					deep.context = context;
					if (err)
						return deferred.reject(err);
					if (response)
						delete response._id;
					else
						return deferred.reject(deep.errors.NotFound());
					//console.log("mongstore (real) put response : ", response);
					deferred.resolve(response);
				});
			});

			
			return deferred.promise()
			.done(function (res){
				if(res && res.headers && res.status && res.body)
					return deep.errors.Internal(res.body, res.status);
			});
		},
		/*patch:function (content, opt) {
			//console.log("ObjectSheet patch : ", content, opt);
			opt = opt || {};
			var self = this;
			deep.utils.decorateUpFrom(this, opt, ["baseURI"]);
			var id = opt.id = opt.id || content.id;
			if(!opt.id)
				return deep.when(deep.errors.Patch("json stores need id on PATCH"));
			var search = {id: id};
			return deep.when(this.get(id, opt))	// check ownership
			.done(function(datas){
				if (!datas || datas.length === 0)
					return deep.errors.NotFound("no items found in collection with : " + id);
				var data = datas;

				if(opt.query)
				{
					deep.query(data, opt.query, { resultType:"full", allowStraightQueries:false })
					.forEach(function(entry){
						entry.value = deep.utils.up(content, entry.value);
						if(entry.ancestor)
							entry.ancestor.value[entry.key] = entry.value;
					});
					delete opt.query;
				}
				else
					deep.utils.up(content, data);
				var schema = self.schema;
				if(schema)
				{
					if(schema._deep_ocm_)
						schema = schema("patch");
					
					var check = self.checkReadOnly(datas, data, opt);
					if(check instanceof Error)
						return deep.when(check);
					var report = deep.validate(data, schema);
					if(!report.valid)
						return deep.errors.PreconditionFail(report);
				}
				
				//console.log("mongo.put : search : ", search, " - obj : ", obj);
				self.collection.findAndModify(search, null , data, {upsert:false, "new":true}, function(err, response){
					if (err)
						return deferred.reject(err);
					if (response)
						delete response._id;
					else
						return deferred.reject(deep.errors.NotFound());
					//console.log("mongstore (real) put response : ", response);
					deferred.resolve(response);
				});

				return data;
			});
        },*/
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
			//var oldQuery = query;
			query += "&limit("+((options.end-options.start)+1)+","+options.start+")";


			if(query[0] == "?")
				query = query.substring(1);
			// compose search conditions
			var x = rqlToMongo.parse(query, parsingDirectives);
			var meta = x[0], search = x[1];
			query = "?"+query;

			// range of non-positive length is trivially empty
			//if (options.limit > options.totalCount)
			//	options.limit = options.totalCount;
			if (meta.limit <= 0) {
				var rangeObject = deep.utils.createRangeObject(0, 0, 0, 0, [], query);
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

			var schema = this.schema;
			// request filtered recordset
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
			self.collection.find(search, meta, function(err, cursor){
				deep.context = context;
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
						deep.context = context;
						var rangeObject = deep.utils.createRangeObject(options.start, 
							Math.max(options.start,options.start+results.length-1), 
							result,
							results.length,
							results,
							query
							);
						//console.log("range result : ", rangeObject);
						//rangeObject.count = results.length;
						//rangeObject.query = query;
						//rangeObject.results = results.concat([]);
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
			//console.log("Mongo del : ", search);
			this.collection.remove(search, { safe:true },function(err, result){
				//console.log("del res : ", err, result);
				deep.context = context;
				if (err)
					return deferred.reject(err);
				deferred.resolve(result === 1);
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
				self.collection.drop(function(err, done)
				{
					//console.log("deep.mongo store flushed");
					if(err)
						return def.reject(err);
					self.ensureIndex({ id: 1 }, { unique: true })
					.done(function(s){
						def.resolve(s);
					})
					.fail(function(e){
						def.reject(e);
					});
				});
			})
			.fail(function(error){
				def.reject(error);
			});
			return def.promise();
		},
		indexes:function(){
			var def = deep.Deferred();
			var self = this;
			this.init()
			.done(function(success){
				self.collection.indexes(function(err, done)
				{
					//console.log("deep.mongo store indexes :", err, done);
					if(err)
						return def.reject(err);
					def.resolve(done || true);
				});
			})
			.fail(function(error){
				def.reject(error);
			});
			return def.promise();
		},
		reIndex:function(){
			var def = deep.Deferred();
			var self = this;
			this.init()
			.done(function(success){
				self.collection.reIndex(function(err, done)
				{
					//console.log("deep.mongo store reIndexed : ", err, done);
					if(err)
						return def.reject(err);
					def.resolve(done || true);
				});
			})
			.fail(function(error){
				def.reject(error);
			});
			return def.promise();
		},
		ensureIndex:function (properties, options) {
			var def = deep.Deferred();
			var self = this;
			this.init()
			.done(function(success){
				self.collection.ensureIndex( properties, options, function(err, res){
					if(err){
						console.error("Failed to ensuring index on mongo database collection : " + url + " : " + collectionName + " error " + err.message);
						return def.reject(err);
					}
					//console.log("MONGO DB : index ensured");
					def.resolve(self);
				});
			})
			.fail(function(error){
				def.reject(error);
			});
			return def.promise();
		},
		count:function (arg){
			var def  = deep.Deferred();
			var context = deep.context;
			this.collection.count(arg, function(err, totalCount) {
				deep.context = context;
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
