# deep-mongo

deep-mongo provides a restful styled mongodb store usable with deepjs.
By other, it make mongodb queriable with RQL (https://github.com/persvr/rql).

See [deep-restful](https://github.com/deepjs/deep-restful) for full API description.


## Install
```shell
	npm install deep-mongo
```

## Usage

```javascript

	var deep = require("deepjs"); // load core
	require("deep-restful"); // load chained API
	require("deep-mongo"); // load driver

	deep.Mongo("items", "mongodb://127.0.0.1:27017/test", "items");

	//...

	deep.restful("items")
	.post({ something:"yes"})
	.slog()
	.put({
		hello:"putted object",
		test:12
	})
	.slog()
	.patch({
		hello:"patched object",
	})
	.slog()
	.get() // get all
	.log();

```


see [testcases](./units/generic.js) for full usage. 