# redis

[![Build Status](https://img.shields.io/travis/amphp/redis/master.svg?style=flat-square)](https://travis-ci.org/amphp/redis)
[![CoverageStatus](https://img.shields.io/coveralls/amphp/redis/master.svg?style=flat-square)](https://coveralls.io/github/amphp/redis?branch=master)
![Stable v1](https://img.shields.io/badge/api-unstable-orange.svg?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)

`amphp/redis` is a non-blocking [`redis`](http://redis.io) client for use with the [`amp`](https://github.com/amphp/amp) concurrency framework.

**Required PHP Version**

- PHP 7

**Installation**

```bash
$ composer require amphp/redis dev-master
```

## Known Bugs

### PubSub

The following example will fail, because `subscribe` immediately assigns
and return a promise, but `unsubscribe` will delete all promises of a given channel.

If you have a good solution for this, please send a PR. It's considered an edge case,
that shouldn't appear in real applications.

```php
$client->subscribe("foo");
$client->unsubscribe("foo");

// If you subscribe again before the unsubscribe was successful,
// the unsubscribe response will succeed this promise.
$promise = $client->subscribe("foo");
```

#### Workaround

```php
$promise = $client->subscribe("foo");
$unsubscribePromise = $client->unsubscribe("foo");

// Wait for the original promise to resolve,
// don't wait on the $unsubscribePromise,
// it resolves immediately when the connection is
// alive, which should always be the case.
yield $promise;

$promise = $client->subscribe("foo");
```
