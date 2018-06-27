
# Swift Client

Swift Client is a library for connecting to an OpenStack Swift storage server.
This project has been forked from https://github.com/stewartml/swift-client.

*Example*
```js
const SwiftClient = require('openstack-swift-client');
const authenticator = new SwiftClient.SwiftAuthenticator('https://orbit.brightbox.com/v1/acc-xxx', 'cli-xxx', 'my-password');

let client = new SwiftClient(authenticator);
let container = client.container('my-container');
container.get('test-file', process.stdout).then(() => {
    console.log("Done!");
});
```
## Installation

    $ npm install --save openstack-swift-client

## Documentation

The main class is `SwiftClient`, which can be imported as follows:

```js
import SwiftClient from 'swift-client';
```
or
```js
const SwiftClient = require('openstack-swift-client');
```

I'm just going to use ES2016 (with async and await) for brevity in this document.


### `SwiftClient` class


#### `SwiftClient(authenticator)`

Creates an instance of `SwiftClient` with the specified authenticator

| Argument | Description |
|----------|-------------|
| `authenticator` | an authenticator instance (see below) |

**Example**
```js
let client = new SwiftClient('https://orbit.brightbox.com/v1/acc-xxx', 'cli-xxx', 'my-password');
```

#### `SwiftClient.SwiftAuthenticator(url, username, password)`

Creates an instance of `SwiftClientAuthenticator` with the specified authentication information.

| Argument | Description |
|----------|-------------|
| `url` | the URL of the server |
| `username` | the username to authenticate with |
| `password` | the password to authenticate with |

**Example**
```js
let client = new SwiftClient(new SwiftClient.SwiftAuthenticator('https://orbit.brightbox.com/v1/acc-xxx', 'cli-xxx', 'my-password'));
```


#### `SwiftClient.KeystoneV3Authenticator(credentials)`

Creates an instance of `KeystoneV3Authenticator` with the specified authentication information.

| Argument | Description |
|----------|-------------|
| `credentials` | credential object |

**Example**
```js
let credentials = {
  "endpointUrl": "https://os.eu-de-darz.msh.host:5000/v3",
  "username": "user",
  "password": "pasword",
  "domainId": "e6efe92d05c2430ea7eb6f626815d0d8",
  "projectId": "ff912d95d2eb46099e755cd268714d37"
}

let client = new SwiftClient(new SwiftClient.KeystoneV3Authenticator(credentials));
```

#### `SwiftClient#list(extra)`

Gets an array of containers.

| Argument | Description |
|----------|-------------|
| `extra` | a hash of additional headers to send (optional) |

**Example**

```js
await client.list();

/* returns:
[
  {name: 'container-name', count: 123, bytes: 12438468},
  (...)
]
*/
```


#### `SwiftClient#create(name, publicRead, meta, extra)`

Creates a container.

| Argument | Description |
|----------|-------------|
| `name` | the name of the container to create |
| `publicRead` | true if the container is to be publicly readable; otherwise, false (optional) |
| `meta` | a hash of meta information to set on the container (optional) |
| `extra` | a hash of additional headers to send (optional) |


**Example**

```js
await client.create('my-container', true, {colour: 'blue'});
```


#### `SwiftClient#update(name, meta, extra)`

Updates the metadata associated with the specified container.

| Argument | Description |
|----------|-------------|
| `name` | the name of the container to update |
| `meta` | a hash of meta information to set on the container |
| `extra` | a hash of additional headers to send (optional) |


**Example**

```js
await client.update('my-container', {colour: 'red'});
```


#### `SwiftClient#meta(name)`

Gets the metadata associated with the specified container.

| Argument | Description |
|----------|-------------|
| `name` | the name of the container to get the metadata for |


**Example**

```js
let meta = await client.meta('my-container');

/*
meta is a hash of metadata, e.g.
{
  colour: 'red'
}
*/
```


#### `SwiftClient#delete(name)`

Deletes the specified container.

| Argument | Description |
|----------|-------------|
| `name` | the name of the container to delete |


**Example**

```js
await client.delete('my-container');
```


#### `SwiftClient#container(name)`

Gets an instance of `SwiftContainer` for the specified container.

| Argument | Description |
|----------|-------------|
| `name` | the name of the container to get a `SwiftContainer` instance for |

**Example**

```js
let container = client.container('my-container');
```

#### `SwiftClient#info()`

Gets cluster configuration parameters


**Example**

```js
await client.info();

/* returns:

{
  bulk_delete: { max_failed_deletes: 1000, max_deletes_per_request: 10000 },
  (...),
  swift:
   { max_file_size: 5368709122,
     account_listing_limit: 10000,
     (...),
     max_container_name_length: 256
   }
}
*/
```

### `SwiftContainer` class

#### `SwiftContainer#list(extra, query)`

Gets an array of objects in the container.

| Argument | Description |
|----------|-------------|
| `extra` | a hash of additional headers to send (optional) |
| `query` | a query string or hash of additional query parameters to send (optional) |

[Query parameters](https://developer.openstack.org/api-ref/object-store/#show-account-details-and-list-containers) can be used to filter the result, e.g. list a pseudo-directory.

**Example**

```js
await container.list();

/* returns:
[
  {
      hash: '03b3aac569fab8b59dcf8f210f8d3bc8',
      last_modified: '2017-03-31T13:27:56.042120',
      bytes: 102400,
      name: 'picture.png',
      content_type: 'application/octet-stream'
  },
  (...)
]
*/
```


#### `SwiftContainer#create(name, stream, meta, extra)`

Creates an object.

| Argument | Description |
|----------|-------------|
| `name` | the name of the object to create |
| `stream` | a stream representing the file to upload |
| `meta` | a hash of meta information to set on the object (optional) |
| `extra` | a hash of additional headers to send (optional) |


**Example**

```js
let stream = fs.createReadStream('darkness-at-noon.txt');

await container.create('books/darkness-at-noon.txt',
  stream, {author: 'Arthur Koestler'});
```



#### `SwiftContainer#get(name, stream)`

Gets an object.

| Argument | Description |
|----------|-------------|
| `name` | the name of the object to get |
| `stream` | a stream to pipe the object to |


**Example**

```js
let stream = fs.createWriteStream('darkness-at-noon.txt');
await container.get('books/darkness-at-noon.txt', stream);
```


#### `SwiftContainer#update(name, meta, extra)`

Updates the metadata associated with the specified object.

| Argument | Description |
|----------|-------------|
| `name` | the name of the object to update |
| `meta` | a hash of meta information to set on the object |
| `extra` | a hash of additional headers to send (optional) |


**Example**

```js
await container.update('books/darkness-at-noon.txt', {year: '1940'});
```


#### `SwiftContainer#meta(name)`

Gets the metadata associated with the specified object.

| Argument | Description |
|----------|-------------|
| `name` | the name of the object to get the metadata for |


**Example**

```js
let meta = await container.meta('books/darkness-at-noon.txt');

/*
meta is a hash of metadata, e.g.
{
  author: 'Arthur Koestler',
  year: '1940'
}
*/
```


#### `SwiftContainer#delete(name, when)`

Deletes the specified object.  If `when` is a `Date`, the object is deleted at that date; if it is a number, the object is deleted after that many seconds; or if it is ommitted, the object is deleted immediately.

| Argument | Description |
|----------|-------------|
| `name` | the name of the object to delete |
| `when` | a `Date` representing when the object is to be deleted, or a number of seconds the object is to be deleted after (optional) |


**Example**

```js
// delete the object in 2 minutes time
await container.delete('books/darkness-at-noon.txt', 120);
```
