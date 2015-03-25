"use strict";
var deep = require("deepjs");
require("deep-restful/lib/store-sheet");
var Store = require("deep-restful/lib/store");
var ranger = require("deep-restful/lib/range");
var mongo = require('mongodb'),
    ObjectID = require('bson/lib/bson/objectid').ObjectID,
    rqlToMongo = require("./rql-to-mongo");


deep.Mongo = deep.Classes(Store, function(protocol, url, collectionName, schema, options) {
    if(schema && this.schema)
        deep.aup(schema, this.schema);
    else
        this.schema = schema || this.schema;
    this.url = url || this.url;
    this.collectionName = collectionName || this.collectionName;
    if (options)
        deep.aup(options, this);
}, {
    url: null,
    collectionName: null,
    _deep_collection_:true,
    index:{
        id:{ keys:{ id:1 }, options:{ unique:true } }
    },
    schema:{
        properties:{
            id:{ type:"string", required:false }
        }
    },
    init: function(options) {
        if (this.initialised) {
            if (this.initialised._deep_deferred_)
                return this.initialised.promise();
            return deep.when(this);
        }
        //console.time("mongo.init");
        //console.log("MONGO STORE INIT ");
        options = options || {};
        var url = options.url || this.url;
        if (!url)
            return deep.errors.Store("Mongo failed to init : no url provided !");
        var def = this.initialised = deep.Deferred();
        var collectionName = options.collectionName || this.collectionName;
        var self = this;
        //console.log("MONGO STORE INIT : try connect : ", url, collectionName);
        deep.wrapNodeAsynch(mongo, "connect", [url])
            .done(function(db) {
                //console.log("MONGO STORE INIT :  connected : ", url, collectionName);
                //db = db[0];
                self.db = function() {
                    return db;
                };
                return deep.wrapNodeAsynch(db, "collection", [collectionName]);
            })
            .done(function(coll) {
                // console.log("MONGO STORE INIT :  collectioned : ", url, collectionName);
                // coll = coll[0];
                self.collection = coll;
                return self.ensureIndex();
            })
            .done(function() {
                console.log("MONGO DB : initialised : ", url, collectionName);
                self.initialised = true;
                def.resolve(self);
            })
            .fail(function(err) {
                console.error('Failed to connect to mongo database ', url, ' - error: ', err);
                def.reject(err);
            });
        //console.log("mongo init: ", this.initialised);
        return def.promise();
    },
    get: function(id, options) {
        // console.log("Mongo : get : ", id, options);
        options = options || {};
        if (id == 'schema') {
            if (this.schema && this.schema._deep_ocm_)
                return this.schema("get");
            return this.schema || {};
        }
        if (!id)
            id = "";
        if (!id || id[0] === "?")
            return this.query(id.substring(1), options);

        var search = {
            id: id
        };
        var meta = {};
        var q = "id=" + id;
        if (options.filter) {
            q += options.filter;
            var x = rqlToMongo.parse(q, {});
            meta = x[0];
            search = x[1];
        }

        return deep.wrapNodeAsynch(this.collection, "findOne", [search, meta])
            .done(function(obj) {
                //obj = obj[0];
                if (obj)
                    delete obj._id;
                else
                    return deep.errors.NotFound("maybe ownership restriction");
                return obj;
            });
    },
    post: function(object, options) {
        options = options || {};
        var id = options.id || object.id;
        //console.log("Post IDs option : ", options.id, object);
        if (!id)
            id = object.id = options.id = ObjectID.createPk().toJSON();
        //console.log("Creating IDs : ", id);
        var self = this;
        return deep.wrapNodeAsynch(self.collection, "insert", [object])
            .fail(function(err) {
                if (err.code == 11000)
                    return deep.errors.Conflict("Mongo post failed conflict :", err);
            })
            .done(function(obj) {
               // console.log("_____________________________    MONGO POST : res ", obj)
                if (obj)
                    obj = obj[0];
                if (obj)
                    delete obj._id;
                else
                    return deep.errors.NotFound();
                return obj;
            });
    },
    put: function(object, options) {
        options = options || {};
        var id = options.id || object.id;
        return deep.wrapNodeAsynch(this.collection, "findAndModify", [{
                id: id
            },
            null /* sort */ , object, {
                upsert: false,
                "new": true
            }
        ])
        .done(function(response) {
            response = response[0];
            if (response)
                delete response._id;
            else
                return deep.errors.NotFound();
            return response;
        });
    },
    patch: function(object, options) {
        options = options || {};
        var id = options.id || object.id;
        return deep.wrapNodeAsynch(this.collection, "findAndModify", [{
                id: id
            },
            null /* sort */ , object, {
                upsert: false,
                "new": true
            }
        ])
            .done(function(response) {
                response = response[0];
                if (response)
                    delete response._id;
                else
                    return deep.errors.NotFound();
                return response;
            });
    },
    MAX_QUERY_LIMIT: 500,
    query: function(query, options) {
        options = options || {};
        if (!query)
            query = "";

        var parsingDirectives = {};

        var deferred = deep.Deferred();
        var self = this;

        var noRange = false;
        if (!options.start && !options.end) {
            noRange = true;
            options.start = 0;
            options.end = this.MAX_QUERY_LIMIT;
        }
        options.start = options.start || 0;
        options.end = options.end || 0;
        if (options.end - options.start > this.MAX_QUERY_LIMIT)
            options.end = options.start + this.MAX_QUERY_LIMIT;
        query += "&limit(" + ((options.end - options.start) + 1) + "," + options.start + ")";

        if (options.filter)
            query += options.filter;
        if (query[0] == "?")
            query = query.substring(1);
        // compose search conditions
        var x = rqlToMongo.parse(query, parsingDirectives);
        var meta = x[0],
            search = x[1];
        query = "?" + query;

        if (meta.limit <= 0) {
            var rangeObject = ranger.createRangeObject(0, 0, 0, 0, [], query);
            rangeObject.results = [];
            rangeObject.count = 0;
            rangeObject.query = query;
            return rangeObject;
        }

        var totalCountPromise = null;
        if (!noRange)
            totalCountPromise = this.count(search);

        var context = deep.Promise.context;
        deep.wrapNodeAsynch(self.collection, "find", [search, meta])
            .done(function(cursor) {
                //cursor = cursor[0];
                cursor.toArray(function(err, results) {
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
                    if (noRange)
                        return deferred.resolve(results);
                    deep.when(totalCountPromise)
                        .done(function(result) {
                            //deep.Promise.context = context;
                            var rangeObject = ranger.createRangeObject(options.start,
                                Math.max(options.start, options.start + results.length - 1),
                                result,
                                results.length,
                                results,
                                query
                            );
                            deferred.resolve(rangeObject);
                        })
                        .fail(function(error) {
                            deferred.reject(error);
                        });
                });
            })
            .fail(function(e) {
                deferred.reject(e);
            });
        //________________________
        //console.log("deep.stores.Mongo will do query : ", query);
        return deferred.promise();
    },
    del: function(id, options) {
        var search = {
            id: id
        }, meta = null, unique = false;

        if(id[0] !== "?")
        {
            id = "?id="+id;
            unique = true;
        }
        id = id.substring(1);
        // compose search conditions
        if(options.filter)
            id += options.filter;
        var x = rqlToMongo.parse(id, {});
        meta = x[0];
        search = x[1];
        meta.safe = true;
        return deep.wrapNodeAsynch(this.collection, "remove", [search, meta])
        .done(function(s) {
            //console.log(" MONGO DEL : ", id, search , meta , s);
            return s === 1;
        });
    },
    range: function(start, end, query) {
        return this.query(query || "", {
            start: start,
            end: end
        });
    },
    flush: function(options) {
        options = options || {};
        var self = this;
        return deep.wrapNodeAsynch(self.collection, "drop", [])
            .done(function() {
                return self.ensureIndex();
            })
            .fail(function(e) {
                console.log("flush error : ", e);
            });
    },
    dropIndex: function(options) {
        options = options || {};
        var self = this;
        return deep.wrapNodeAsynch(self.collection, "dropIndex", [])
            .done(function() {
                if (options.ensureIndex !== false)
                    return self.ensureIndex();
            })
            .fail(function(e) {
                console.log("flush error : ", e);
            });
    },
    indexes: function() {
        var self = this;
        return deep.wrapNodeAsynch(self.collection, "indexes", [])
            .done(function(indexes) {
                return indexes;
            });
    },
    reIndex: function() {
        var self = this;
        return deep.wrapNodeAsynch(self.collection, "reIndex", [])
        .done(function(indexes) {
            return indexes;
        });
    },
    ensureIndex: function(keys, options) {
        var self = this;
        if(!keys)
        {
            var promises = [];
            for(var i in self.index)
                promises.push(deep.wrapNodeAsynch(self.collection, "ensureIndex", [self.index[i].keys, self.index[i].options]));
            return deep.all(promises);
        }
        return deep.wrapNodeAsynch(self.collection, "ensureIndex", [keys, options])
        .done(function(indexes) {
            return indexes;
        });
    },
    count: function(arg) {
        var self = this;
        return deep.wrapNodeAsynch(self.collection, "count", [arg])
        .done(function(totalCount) {
            return totalCount;
        });
    }
}, deep.store.fullSheet);

deep.Mongo.drop = function(url){
    return deep.wrapNodeAsynch(mongo, "connect", [url])
    .done(function(db) {
        //console.log("MONGO STORE DROP DB : ", url);
        return deep.wrapNodeAsynch(db, "dropDatabase", []);
    });
};
deep.coreUnits = deep.coreUnits || [];
deep.coreUnits.push("js::deep-mongo/units/generic");

module.exports = deep.Mongo;



