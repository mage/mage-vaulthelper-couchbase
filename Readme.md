mage-vaulthelper-couchbase
==========================

Accessing vault backends directly is generally considered
to be tedious in MAGE. This will help you by creating
object instances to access Couchbase directly, so that you may
do N1QL and view query calls.

It will also allow you to define how many N1QL query calls
should run at once, and how many should be queued.

Installation
------------

```shell
npm install --save mage-vaulthelper-couchbase
```

Usage
------

**Examples are written in TypeScript, but this library can also be used in pure JavaScript**

First, you will need to create a helper instance somewhere.

> ./lib/vaults/index.ts

```typescript
import * as couchbase from 'mage-vaulthelper-couchbase'

export const volatileVault = new couchbase.Helper('volatileVault')
export const userVault = new couchbase.Helper('userVault')
export const gameVault = new couchbase.Helper('gameVault')
```

You will probably want to add all your other vault helpers
in here, if you have any.

Then, to access from your modules:

> ./lib/modules/players/index.ts

```typescript
import * as mage from 'mage'
import {
  userVault
} from '../../vaults'

exports.list = async function (state: mage.core.IState) {
  const results = await userVault.query('select 1') // or another query

  mage.logger.debug.data({
    results
  }).log('got results')

  return results
}
```

Acknowledgements
----------------

@AlmirKadric for writing the initial version of this helper.

License
-------

MIT
