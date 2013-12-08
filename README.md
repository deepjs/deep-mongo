deep-mongo provides a restful styled mongodb store usable with deep and queriable in RQL (https://github.com/persvr/rql).

## Required

* deepjs >= v0.9.9
* mongodb >=0.9.9-8
* bson >=0.2.3
* node >= 0.10.0

## Install
```shell
	npm install deep-mongo
```

## Usage

```javascript

	var deep = require("deepjs");
	require("deep-mongo").create("items", "mongodb://127.0.0.1:27017/test", "items");

	//...

	deep.store("items")
	.post({ weeeee:"gdgdgdgdgdggd "})
	.log()
	.put({
		hello:"putted object",
		test:12,
		id:"525c4807d75ffe599c3ca002"
	})
	.log()
	.get("525c4807d75ffe599c3ca002")
	.log()
	.patch({
		hello:"patched object",
		id:"525c4807d75ffe599c3ca002"
	})
	.log()
	.get("?id=525c4807d75ffe599c3ca002")
	.log();

```


see [testcases](./units/generic.js) for full usage. full docs coming soon.