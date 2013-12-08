if (typeof define !== 'function') {
    var define = require('amdefine')(module);
}

define(["require","deepjs/deep", "deepjs/deep-unit"], function (require, deep, Unit) {
    
    //_______________________________________________________________ GENERIC STORE TEST CASES
    var postTest = {
        id:"id123",
        title:"hello",
        order:2
    };
    var putTest = {
        id:"id123",
        order:2,
        otherVar:"yes"
    };
    var patchTest = {
        id:"id123",
        order:4,
        otherVar:"yes",
        newVar:true
    };

    var unit = {
        title:"deep-mongo generic testcases",
        stopOnError:false,
        setup:function(){
            delete deep.context.session;
            var store = deep.store.Mongo.create("test", "mongodb://127.0.0.1:27017/testcases", "tests");
            return deep.when(store.init())
            .done(function(){
                return store.flush();
            })
            .done(function(){
                return store;
            });
        },
        clean:deep.compose.before(function (){
            this.context.flush();
            delete deep.context.session;
            delete deep.protocoles.test;
        }),
        tests : {
            post:function(){
                return deep.store("test")
                .post( postTest )
                .equal( postTest )
                .get("id123")
                .equal(postTest);
            },
            postErrorIfExists:function(){
                return deep.store("test")
                .post( postTest )
                .fail(function(error){
                    if(error.status == 409)   // conflict
                        return "lolipop";
                })
                .equal("lolipop");
            },
            put:function(){
                // post
                return deep.store("test")
                // put
                .put(putTest)
                .equal( putTest )
                .get("id123")
                .equal( putTest );
            },
            patch:function(){
                // post
                return deep.store("test")
                .patch({
                    order:4,
                    newVar:true,
                    id:"id123"
                })
                .equal(patchTest)
                .get("id123")
                .equal(patchTest);
            },
            query:function(){
                // post
                return deep.store("test")
                // query
                .get("?order=4")
                .equal([patchTest]);
            },
            del:function () {
                var delDone = false;
                return deep.store("test")
                .del("id123")
                .done(function (argument) {
                    delDone = true;
                })
                .get("id123")
                .fail(function(error){
                    if(delDone && error.status == 404)
                        return true;
                });
            },
            range:function(){
                var self = this;
                return deep.store(this)
                .run("flush")
                .done(function(s){
                    this.post({title:"hello"})
                    .post({title:"hell"})
                    .post({title:"heaven"})
                    .post({title:"helicopter"})
                    .post({title:"heat"})
                    .post({title:"here"})
                    .range(2,4)
                    .done(function(range){
                        deep.utils.remove(range.results,".//id");
                        deep.chain.remove(this, ".//id");
                    })
                    .equal({ _deep_range_: true,
                      total: 6,
                      count: 3,
                      results:
                       [ { title: 'heaven' },
                         { title: 'helicopter' },
                         { title: 'heat' } ],
                      start: 2,
                      end: 4,
                      hasNext: true,
                      hasPrevious: true,
                      query: '?&limit(3,2)'
                    })
                    .valuesEqual([
                        { title: 'heaven' },
                        { title: 'helicopter' },
                        { title: 'heat' }
                    ]);
                });
            },
            rangeWithQuery:function(){
                
                return deep.store(this)
                .run("flush")
                .post({id:"u1", count:1})
                .post({id:"u2", count:2})
                .post({id:"u3", count:3})
                .post({id:"u4", count:4})
                .post({id:"u5", count:5})
                .post({id:"u6", count:6})
                .range(2,4, '?count=ge=3')
                .equal({ _deep_range_: true,
                  total: 4,
                  count: 2,
                  results:
                    [
                        { id:"u5", count:5 },
                        { id:"u6", count:6 }
                    ],
                  start: 2,
                  end: 3,
                  hasNext: false,
                  hasPrevious: true,
                  query: "?count=ge=3&limit(3,2)"
                })
                .valuesEqual([
                    { id:"u5", count:5 },
                    { id:"u6", count:6 }
                ]);
            },
            rpc:function(){
                var checker = {};
                this.methods = {
                    testrpc:function(handler, arg1, arg2)
                    {
                        checker.throughRPC = true;
                        checker.args = [arg1, arg2];
                        checker.count = this.count;
                        this.decorated = "hello rpc";
                        return handler.save();
                    }
                };
                return deep.store(this)
                .rpc("testrpc", [1456, "world"], "u1")
                .equal({ id:"u1", count:1, decorated:"hello rpc" })
                .valuesEqual({ id:"u1", count:1, decorated:"hello rpc" })
                .get("u1")
                .equal({ id:"u1", count:1, decorated:"hello rpc" })
                .deep(checker)
                .equal({
                    throughRPC:true,
                    args:[1456, "world"],
                    count:1
                });
            },
            rpcErrorIfNotExists:function(){
                var checker = {};
                this.methods = {
                    testrpc:function(handler, arg1, arg2)
                    {
                        checker.throughRPC = true;
                        checker.args = [arg1, arg2];
                        checker.count = this.count;
                        this.decorated = "hello rpc";
                        return handler.save();
                    }
                };
                return deep.store(this)
                .rpc("testrpc", [1456, "world"], "u24")
                .fail(function(error){
                     if(error.status == 404)    // not found
                        return "lolipop";
                })
                .equal("lolipop");
            },
            rpcMethodNotAllowed:function(){
                var checker = {};
                this.methods = {
                    testrpc:function(handler, arg1, arg2)
                    {
                        checker.throughRPC = true;
                        checker.args = [arg1, arg2];
                        checker.count = this.count;
                        this.decorated = "hello rpc";
                        return handler.save();
                    }
                };
                return deep.store(this)
                .rpc("testrpco", [1456, "world"], "u1")
                .fail(function(error){
                     if(error.status == 405)    // not found
                        return "lolipop";
                })
                .equal("lolipop");
            },
            privateGet:function(){
                this.schema = {
                    properties:{
                        password:{ type:"string", "private":true }
                    }
                };
                return deep.store(this)
                .run("flush")
                .post({ id:"u1", email:"gilles.coomans@gmail.com", password:"test"})
                .get("u1")
                .equal({ id:"u1", email:"gilles.coomans@gmail.com" })
                .valuesEqual({ id:"u1", email:"gilles.coomans@gmail.com" });
            },
            privateQuery:function(){
                return deep.store("test")
                .get("?id=u1")
                .equal([{ id:"u1", email:"gilles.coomans@gmail.com" }]);
            },
            privatePost:function(){
                return deep.store("test")
                .del("u2")
                .post({ id:"u2", email:"john.doe@gmail.com", password:"test"})
                .equal({ id:"u2", email:"john.doe@gmail.com" })
                .del("u2");
           },
           privatePatch:function(){
                return deep.store("test")
                .patch({ id:"u1", email:"john.doe@gmail.com", userID:"u1" })
                .equal({ id:"u1", email:"john.doe@gmail.com" , userID:"u1" });
           },
           readOnly:function(){
                deep.protocoles.test.schema = {
                    properties:{
                        email:{ readOnly:true, type:"string" }
                    }
                };
                return deep.store("test")
                .patch({ id:"u1", email:"should produce error" })
                .fail(function(e){
                    if(e.status == 412)
                        return "lolipop";
                })
                .equal("lolipop");
            },
            putErrorIfNotExists:function(){
                deep.protocoles.test.schema = {};
                return deep.store("test")
                .put({ id:"u35", email:"gilles@gmail.com" })
                .fail(function(error){
                     if(error.status == 404)    // not found
                        return "lolipop";
                })
                .equal("lolipop");
            },
            patchErrorIfNotExists:function(){
                deep.protocoles.test.schema = {};
                return deep.store("test")
                .patch({ id:"u35", email:"gilles@gmail.com" })
                .fail(function(error){
                     if(error.status == 404)    // not found
                        return "lolipop";
                })
                .equal("lolipop");
            },
            putWithQuery:function(){
                deep.protocoles.test.schema = {};
                return deep.store("test")
                .put("gilles@gmail.com", { id:"u1", query:"/email"})
                .equal({ id:"u1", email:"gilles@gmail.com" ,password: 'test', userID:"u1" })
                .valuesEqual({ id:"u1", email:"gilles@gmail.com" ,password: 'test', userID:"u1"})
                .get("u1")
                .equal({ id:"u1", email:"gilles@gmail.com", password: 'test', userID:"u1" });
            },
            patchWithQuery:function(){
                deep.protocoles.test.schema = {};
                return deep.store("test")
                .patch("michel@gmail.com", { id:"u1", query:"/email"})
                .equal({ id:"u1", email:"michel@gmail.com" ,password: 'test', userID:"u1" })
                .valuesEqual({ id:"u1", email:"michel@gmail.com" ,password: 'test', userID:"u1"})
                .get("u1")
                .equal({ id:"u1", email:"michel@gmail.com", password: 'test', userID:"u1" });
            },
            postValidationFailed:function(){
                deep.protocoles.test.schema = {
                    properties:{
                        id:{ type:"string", required:true },
                        title:{ type:"number", required:true }
                    }
                };
                return deep.store("test")
                .post({ id:"u1", title:"gilles.coomans@gmail.com" })
                .fail(function(error){
                    if(error && error.status == 412)   // Precondition
                        return "lolipop";
                })
                .equal("lolipop");
            },
            delFalseIfNotExists:function(){
                return deep.store("test")
                .del('u45')
                .equal(false);
            },
 
           ownerPatchFail:function(){
                deep.protocoles.test.schema = {
                    ownerRestriction:true,
                    properties:{
                        password:{ type:"string", "private":true }
                    }
                };

                return deep.store("test")
                .patch({ id:"u1", email:"john.piperzeel@gmail.com"  })
                .fail(function(e){
                    if(e.status == 403)
                        return "ksss";
                })
                .equal("ksss");
            },
            ownerPatchOk:function(){
                deep.context.session = {
                    remoteUser:{ id:"u1" }
                };

                return deep.store("test")
                .patch({ id:"u1", email:"john.piperzeel@gmail.com"  })
                .equal({ id:"u1", email:"john.piperzeel@gmail.com", userID:"u1"});
            },
            ownerPutFail:function(){
                deep.context.session = {
                    remoteUser:{ id:"u2" }
                };
                return deep.store("test")
                .put({ id:"u1", email:"john.doe@gmail.com", userID:"u1" })
                .fail(function(e){
                    if(e.status == 403)
                        return true;
                })
                .equal(true);
            },
            ownerPutOk:function(){
                deep.context.session = {
                    remoteUser:{ id:"u1" }
                };

                return deep.store("test")
                .put({ id:"u1", email:"john.doe@gmail.com", userID:"u1" })
                .equal({ id:"u1", email:"john.doe@gmail.com", userID:"u1" });
            },
            ownerDelFail:function(){
                deep.context.session = {
                    remoteUser:{ id:"u2" }
                };
                return deep.store("test")
                .del("u1")
                .equal(false)
                .context("session", {
                    remoteUser:{ id:"u1" }
                })
                .get("u1")
                .equal({ id:"u1", email:"john.doe@gmail.com", userID:"u1" });
            },
            ownerDelOk:function(){
                deep.context.session = {
                    remoteUser:{ id:"u1" }
                };
                return deep.store("test")
                .del("u1")
                .equal(true)
                .get("u1")
                .fail(function(e){
                    if(e.status == 404)
                        return "lolipop";
                })
                .equal("lolipop");
            }
        }
    };

    return new Unit(unit);
});
