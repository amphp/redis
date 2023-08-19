# redis

[![Build Status](https://img.shields.io/travis/amphp/redis/master.svg?style=flat-square)](https://travis-ci.org/amphp/redis)
[![Coverage Status](https://img.shields.io/coveralls/amphp/redis/master.svg?style=flat-square)](https://coveralls.io/github/amphp/redis?branch=master)
![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)

`amphp/redis` provides non-blocking access to [Redis](http://redis.io) instances.
All I/O operations are handled by the [Amp](https://github.com/amphp/amp) concurrency framework, so you should be familiar with the basics of it.

## Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require amphp/redis
```

## Usage

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use Amp\Redis\RedisConfig;
use Amp\Redis\Redis;
use Amp\Redis\SocketRedisClient;

Amp\Loop::run(static function () {
    $redis = new Redis(new SocketRedisClient(RedisConfig::fromUri('redis://')));

    yield $redis->set('foo', '21');
    $result = yield $redis->increment('foo', 21);

    \var_dump($result); // int(42)
});
```

## Security

If you discover any security related issues, please use the private security issue reporter instead of using the public issue tracker.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
