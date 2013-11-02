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
        setup:function(){
            return require("deep-mongo").create(null, "mongodb://127.0.0.1:27017/test", "items3").init();
        },
        clean:deep.compose.before(function (){
            this.context.flush();
        }),
        tests : {
            post:function(){
                return deep.store(this)
                .post( postTest )
                .equal( postTest )
                .get("id123")
                .equal(postTest);
            },
            put:function(){
                // post
                return deep.store(this)
                // put
                .put(putTest)
                .equal( putTest )
                .get("id123")
                .equal( putTest );
            },
            patch:function(){
                // post
                return deep.store(this)
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
                return deep.store(this)
                // query
                .get("?order=4")
                .equal([patchTest]);
            },
            del:function () {
                var delDone = false;
                return deep.store(this)
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
                //.run("flush")
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
                      query: '&limit(3,2)'
                    })
                    .valuesEqual([
                        { title: 'heaven' },
                        { title: 'helicopter' },
                        { title: 'heat' }
                    ]);
                });
            }
        }
    };



/*
deep.store("myobjects")
.patch({
    id:"id1381690769563",
    test:"hello",
    fdfsdfsddsfsdfsfdfsd:"11111111111"
})
.rpc("first", ["hhhhh","gggggg"], "id1381690769563")
.get()
.bulk([
    {to:"id1381690769563", method:"patch", body:{name:"updated 2"}},
    {to:"id1381690769563", method:"get"},
    {to:"id1381690769563", method:"rpc", body:{ args:["hello","blkrpc"], method:"first" }}
])
.log();
*/
    unit = new Unit(unit);
    return unit;
});
