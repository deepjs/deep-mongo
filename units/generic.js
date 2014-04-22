var unit = {
    title: "deep-mongo/units/generic",
    stopOnError: false,
    setup: function() {
        delete deep.context.session;
        return deep.store.Mongo.create(null, "mongodb://127.0.0.1:27017/testcases", "tests");
    },
    clean: deep.compose.before(function() {
        return deep.store.Mongo.dropDB("mongodb://127.0.0.1:27017/testcases");
    }),
    tests: {
        post: function() {
            return deep
                .store(this)
                .flush()
                //.log("_________________________  MONGO POST")
                .post({
                    id: "id123",
                    title: "hello",
                    order: 2
                })
                .equal({
                    id: "id123",
                    title: "hello",
                    order: 2
                })
                .get("id123")
                .equal({
                    id: "id123",
                    title: "hello",
                    order: 2
                });
        },
        postErrorIfExists: function() {
            return deep.store(this)
                .post({
                    id: "id123",
                    title: "hello",
                    order: 2
                })
                .fail(function(error) {
                    if (error.status == 409) // conflict
                        return "lolipop";
                })
                .equal("lolipop");
        },
        put: function() {
            return deep
                .store(this)
                .put({
                    id: "id123",
                    order: 2,
                    title: "yes"
                })
                .equal({
                    id: "id123",
                    order: 2,
                    title: "yes"
                })
                .get("id123")
                .equal({
                    id: "id123",
                    order: 2,
                    title: "yes"
                });
        },
        patch: function() {
            return deep.store(this)
                .patch({
                    order: 4,
                    newVar: true,
                    id: "id123"
                })
                .equal({
                    id: "id123",
                    order: 4,
                    title: "yes",
                    newVar: true
                })
                .get("id123")
                .equal({
                    id: "id123",
                    order: 4,
                    title: "yes",
                    newVar: true
                });
        },
        query: function() {
            return deep.store(this)
                .get("?order=4")
                .equal([{
                    id: "id123",
                    order: 4,
                    title: "yes",
                    newVar: true
                }]);
        },
        del: function() {
            var delDone = false;
            return deep.store(this)
                .del("id123")
                .done(function(argument) {
                    delDone = true;
                })
                .get("id123")
                .fail(function(error) {
                    if (delDone && error.status == 404)
                        return true;
                });
        },
        range: function() {
            var self = this;
            return deep.store(this)
            ///.delay(400)
            .run("flush")
            //.log()
            .post({
                title: "hello"
            })
            .post({
                title: "hell"
            })
            .post({
                title: "heaven"
            })
            .post({
                title: "helicopter"
            })
            .post({
                title: "heat"
            })
            .post({
                title: "here"
            })
            .range(2, 4)
            .done(function(range) {
                deep.utils.remove(range.results, ".//id");
                deep.chain.remove(this, ".//id");
            })
            .equal({
                _deep_range_: true,
                total: 6,
                count: 3,
                results: [{
                    title: 'heaven'
                }, {
                    title: 'helicopter'
                }, {
                    title: 'heat'
                }],
                start: 2,
                end: 4,
                hasNext: true,
                hasPrevious: true,
                query: '?&limit(3,2)'
            })
            .valuesEqual([{
                title: 'heaven'
            }, {
                title: 'helicopter'
            }, {
                title: 'heat'
            }]);
        },
        rangeWithQuery: function() {

            return deep.store(this)
                .run("flush")
                .post({
                    id: "u1",
                    count: 1
                })
                .post({
                    id: "u2",
                    count: 2
                })
                .post({
                    id: "u3",
                    count: 3
                })
                .post({
                    id: "u4",
                    count: 4
                })
                .post({
                    id: "u5",
                    count: 5
                })
                .post({
                    id: "u6",
                    count: 6
                })
                .range(2, 4, '?count=ge=3')
                .equal({
                    _deep_range_: true,
                    total: 4,
                    count: 2,
                    results: [{
                        id: "u5",
                        count: 5
                    }, {
                        id: "u6",
                        count: 6
                    }],
                    start: 2,
                    end: 3,
                    hasNext: false,
                    hasPrevious: true,
                    query: "?count=ge=3&limit(3,2)"
                })
                .valuesEqual([{
                    id: "u5",
                    count: 5
                }, {
                    id: "u6",
                    count: 6
                }]);
        },
        rpc: function() {
            var checker = {};
            this.methods = {
                testrpc: function(handler, arg1, arg2) {
                    checker.throughRPC = true;
                    checker.args = [arg1, arg2];
                    checker.count = this.count;
                    this.decorated = "hello rpc";
                    return handler.save();
                }
            };
            return deep.store(this)
                .rpc("testrpc", [1456, "world"], "u1")
                .equal({
                    id: "u1",
                    count: 1,
                    decorated: "hello rpc"
                })
                .valuesEqual({
                    id: "u1",
                    count: 1,
                    decorated: "hello rpc"
                })
                .get("u1")
                .equal({
                    id: "u1",
                    count: 1,
                    decorated: "hello rpc"
                })
                .deep(checker)
                .equal({
                    throughRPC: true,
                    args: [1456, "world"],
                    count: 1
                });
        },
        rpcErrorIfNotExists: function() {
            var checker = {};
            this.methods = {
                testrpc: function(handler, arg1, arg2) {
                    checker.throughRPC = true;
                    checker.args = [arg1, arg2];
                    checker.count = this.count;
                    this.decorated = "hello rpc";
                    return handler.save();
                }
            };
            return deep.store(this)
                .rpc("testrpc", [1456, "world"], "u24")
                .fail(function(error) {
                    if (error.status == 404) // not found
                        return "lolipop";
                })
                .equal("lolipop");
        },
        rpcMethodNotAllowed: function() {
            var checker = {};
            this.methods = {
                testrpc: function(handler, arg1, arg2) {
                    checker.throughRPC = true;
                    checker.args = [arg1, arg2];
                    checker.count = this.count;
                    this.decorated = "hello rpc";
                    return handler.save();
                }
            };
            return deep.store(this)
                .rpc("testrpco", [1456, "world"], "u1")
                .fail(function(error) {
                    if (error.status == 405) // not found
                        return "lolipop";
                })
                .equal("lolipop");
        },
        privateGet: function() {
            this.schema = {
                properties: {
                    password: {
                        type: "string",
                        "private": true
                    }
                }
            };
            return deep.store(this)
                .run("flush")
                .post({
                    id: "u1",
                    email: "gilles.coomans@gmail.com",
                    password: "test"
                })
                .get("u1")
                .equal({
                    id: "u1",
                    email: "gilles.coomans@gmail.com"
                })
                .valuesEqual({
                    id: "u1",
                    email: "gilles.coomans@gmail.com"
                });
        },
        privateQuery: function() {
            return deep.store(this)
                .get("?id=u1")
                .equal([{
                    id: "u1",
                    email: "gilles.coomans@gmail.com"
                }]);
        },
        privatePost: function() {
            return deep.store(this)
                .del("u2")
                .post({
                    id: "u2",
                    email: "john.doe@gmail.com",
                    password: "test"
                })
                .equal({
                    id: "u2",
                    email: "john.doe@gmail.com"
                })
                .del("u2");
        },
        privatePatch: function() {
            return deep.store(this)
                .patch({
                    id: "u1",
                    email: "john.doe@gmail.com",
                    userID: "u1"
                })
                .equal({
                    id: "u1",
                    email: "john.doe@gmail.com",
                    userID: "u1"
                });
        },
        readOnly: function() {
            this.schema = {
                properties: {
                    email: {
                        readOnly: true,
                        type: "string"
                    }
                }
            };
            return deep.store(this)
                .patch({
                    id: "u1",
                    email: "should produce error"
                })
                .fail(function(e) {
                    if (e.status == 412)
                        return "lolipop";
                })
                .equal("lolipop");
        },
        putErrorIfNotExists: function() {
            this.schema = {};
            return deep.store(this)
                .put({
                    id: "u35",
                    email: "gilles@gmail.com"
                })
                .fail(function(error) {
                    if (error.status == 404) // not found
                        return "lolipop";
                })
                .equal("lolipop");
        },
        patchErrorIfNotExists: function() {
            this.schema = {};
            return deep.store(this)
                .patch({
                    id: "u35",
                    email: "gilles@gmail.com"
                })
                .fail(function(error) {
                    if (error.status == 404) // not found
                        return "lolipop";
                })
                .equal("lolipop");
        },
        putWithQuery: function() {
            this.schema = {};
            return deep.store(this)
                .get("u1")
                .put("gilles@gmail.com", "u1/email")
                .equal({
                    id: "u1",
                    email: "gilles@gmail.com",
                    password: 'test',
                    userID: "u1"
                })
                .valuesEqual({
                    id: "u1",
                    email: "gilles@gmail.com",
                    password: 'test',
                    userID: "u1"
                })
            //.get("u1")
            //.equal({ id:"u1", email:"gilles@gmail.com", password: 'test', userID:"u1" });
        },
        patchWithQuery: function() {
            this.schema = {};
            return deep.store(this)
                .patch("michel@gmail.com", "u1/email")
                .equal({
                    id: "u1",
                    email: "michel@gmail.com",
                    password: 'test',
                    userID: "u1"
                })
                .valuesEqual({
                    id: "u1",
                    email: "michel@gmail.com",
                    password: 'test',
                    userID: "u1"
                })
                .get("u1")
                .equal({
                    id: "u1",
                    email: "michel@gmail.com",
                    password: 'test',
                    userID: "u1"
                });
        },
        postValidationFailed: function() {
            this.schema = {
                properties: {
                    id: {
                        type: "string",
                        required: true
                    },
                    title: {
                        type: "number",
                        required: true
                    }
                }
            };
            return deep.store(this)
                .post({
                    id: "u1",
                    title: "gilles.coomans@gmail.com"
                })
                .fail(function(error) {
                    if (error && error.status == 412) // Precondition
                        return "lolipop";
                })
                .equal("lolipop");
        },
        delFalseIfNotExists: function() {
            return deep.store(this)
                .del('u45')
                .equal(false);
        },
        ownerPatchFail: function() {
            this.schema = {
                ownerRestriction: "userID"
            };
            return deep.store(this)
                .context("session", {
                    user: {
                        id: "u3"
                    }
                })
                .patch({
                    id: "u1",
                    email: "john.piperzeel@gmail.com"
                })
                .fail(function(e) {
                    if (e.status == 404) // because restriction
                        return "ksss";
                })
                .equal("ksss");
        },
        ownerPatchOk: function() {
            return deep.store(this)
                .context("session", {
                    user: {
                        id: "u1"
                    }
                })
                .patch({
                    id: "u1",
                    email: "john.piperzeel@gmail.com"
                })
                .equal({
                    id: "u1",
                    email: "john.piperzeel@gmail.com",
                    password: 'test',
                    userID: "u1"
                });
        },
        ownerPutFail: function() {
            return deep.store(this)
                .context("session", {
                    user: {
                        id: "u2"
                    }
                })
                .put({
                    id: "u1",
                    email: "john.doe@gmail.com",
                    userID: "u1"
                })
                .fail(function(e) {
                    if (e.status == 404) // because restriction
                        return true;
                })
                .equal(true);
        },
        ownerPutOk: function() {
            return deep.store(this)
                .context("session", {
                    user: {
                        id: "u1"
                    }
                })
                .put({
                    id: "u1",
                    email: "john.doe@gmail.com",
                    userID: "u1"
                })
                .equal({
                    id: "u1",
                    email: "john.doe@gmail.com",
                    userID: "u1"
                });
        },
        ownerDelFail: function() {
            return deep.store(this)
                .context("session", {
                    user: {
                        id: "u2"
                    }
                })
                .del("u1")
                .equal(false)
                .context("session", {
                    user: {
                        id: "u1"
                    }
                })
                .get("u1")
                .equal({
                    id: "u1",
                    email: "john.doe@gmail.com",
                    userID: "u1"
                });
        },
        ownerDelOk: function() {
            return deep.store(this)
                .context("session", {
                    user: {
                        id: "u1"
                    }
                })
                .del("u1")
                .equal(true)
                .get("u1")
                .fail(function(e) {
                    if (e.status == 404)
                        return "lolipop";
                })
                .equal("lolipop");
        },
        filterGet: function() {
            this.schema = {
                filter: "&status=published"
            };
            deep.context.session = {};
            return deep.store(this)
                .post({
                    id: "u23",
                    title: "hello",
                    status: "draft"
                })
                .get("u23")
                .fail(function(e) {
                    if (e.status == 404)
                        return "yolli";
                })
                .equal('yolli');
        },
        filterGet2: function() {
            this.schema = {
                filter: "&status=draft"
            };
            return deep.store(this)
                .get("u23")
                .equal({
                    id: "u23",
                    title: "hello",
                    status: "draft"
                });
        },
        filterQuery: function() {
            this.schema = {
                filter: "&status=published"
            };
            return deep.store(this)
                .get("?id=i1")
                .equal([]);
        },
        filterQuery2: function() {
            this.schema = {
                filter: "&status=draft"
            };
            return deep.store(this)
                .get("?id=u23")
                .equal([{
                    id: "u23",
                    title: "hello",
                    status: "draft"
                }]);
        },
        filterDel: function() {
            this.schema = {
                filter: "&status=published"
            };
            return deep.store(this)
                .del("u23")
                .equal(false);
        },
        filterDel2: function() {
            this.schema = {
                filter: "&status=draft"
            };
            return deep.store(this)
                .del("u23")
                .equal(true);
        },
        transformers:function(){
            this.schema = {
                properties:{
                    label:{ 
                        type:"string",
                        transformers:[
                        function(node){
                            return node.value+":hello"
                        }]
                    }
                }
            };
            return deep.store(this)
            .post({ label:"weee", status:"draft" })
            .done(function(s){
                delete s.id;
            })
            .equal({ label:"weee:hello", status:"draft" });
        }
    }
};

module.exports = unit;