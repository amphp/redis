# amp-redis [![Build Status](https://travis-ci.org/amphp/redis.svg?branch=master)](https://travis-ci.org/amphp/redis) [![](https://img.shields.io/badge/amp--chat-join%20Two%20Crowns-blue.svg)](https://dev.kelunik.com)

amp-redis is an async [redis](http://redis.io) client based
on the [amp](https://github.com/amphp/amp) framework.

> **Note**: This library is still under development and subject to change.
Use at your own risk!

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