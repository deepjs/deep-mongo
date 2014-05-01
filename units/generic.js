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
                .rest(this)
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
            return deep.rest(this)
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
                .rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
            ///.delay(400)
            .flush()
            //.log()
            .post({
                title: "hello",
                id:'e1'
            })
            .post({
                title: "hell",
                id:'e2'
            })
            .post({
                title: "heaven",
                id:'e3'
            })
            .post({
                title: "helicopter",
                id:'e4'
            })
            .post({
                title: "heat",
                id:'e5'
            })
            .post({
                title: "here",
                id:'e6'
            })
            .range(2, 4)
            .equal({
                _deep_range_: true,
                total: 6,
                count: 3,
                results: [{
                    title: 'heaven',
                    id:'e3'
                }, {
                    title: 'helicopter',
                    id:'e4'
                }, {
                    title: 'heat',
                    id:'e5'
                }],
                start: 2,
                end: 4,
                hasNext: true,
                hasPrevious: true,
                query: '?&limit(3,2)'
            });
        },
        rangeWithQuery: function() {

            return deep.rest(this)
                .flush()
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
                });
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
            return deep.rest(this)
                .rpc("testrpc", [1456, "world"], "u1")
                .equal({
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
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
                .flush()
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
        },
        privateQuery: function() {
            return deep.rest(this)
                .get("?id=u1")
                .equal([{
                    id: "u1",
                    email: "gilles.coomans@gmail.com"
                }]);
        },
        privatePost: function() {
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
                .get("u1")
                .put("gilles@gmail.com", "u1/email")
                .equal({
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
            return deep.rest(this)
                .patch("michel@gmail.com", "u1/email")
                .equal({
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
            return deep.rest(this)
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
            return deep.rest(this)
                .del('u45')
                .equal(false);
        },
        ownerPatchFail: function() {
            this.schema = {
                ownerRestriction: "userID"
            };
            return deep.rest(this)
                .toContext("session", {
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
            return deep.rest(this)
                .toContext("session", {
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
            return deep.rest(this)
                .toContext("session", {
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
            return deep.rest(this)
                .toContext("session", {
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
            return deep.rest(this)
                .toContext("session", {
                    user: {
                        id: "u2"
                    }
                })
                .del("u1")
                .equal(false)
                .toContext("session", {
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
            return deep.rest(this)
                .toContext("session", {
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
            return deep.rest(this)
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
            return deep.rest(this)
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
            return deep.rest(this)
                .get("?id=i1")
                .equal([]);
        },
        filterQuery2: function() {
            this.schema = {
                filter: "&status=draft"
            };
            return deep.rest(this)
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
            return deep.rest(this)
                .del("u23")
                .equal(false);
        },
        filterDel2: function() {
            this.schema = {
                filter: "&status=draft"
            };
            return deep.rest(this)
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
            return deep.rest(this)
            .post({ label:"weee", status:"draft" })
            .done(function(s){
                delete s.id;
            })
            .equal({ label:"weee:hello", status:"draft" });
        }
    }
};

module.exports = unit;