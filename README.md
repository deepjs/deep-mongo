deep-mongo provides a restful styled mongodb store usable with deep and queriable in RQL (https://github.com/persvr/rql).

## Required

* deepjs >= v0.9.4
* node >= 0.10.0

## Install
```shell
	git clone https://github.com/deepjs/deep-mongo
	cd deep-mongo
	npm install
```

## Usage

```javascript

	require("deep-mongo").create("mongo", "mongodb://127.0.0.1:27017/test", "items");

	deep.store("mongo")
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