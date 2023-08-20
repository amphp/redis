# redis

AMPHP is a collection of event-driven libraries for PHP designed with fibers and concurrency in mind.
This package provides non-blocking access to [Redis](http://redis.io) instances.
All I/O operations are handled by [Revolt](https://revolt.run) event loop, so you should be familiar with the basics of it.

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
use Amp\Redis\Connection\ReconnectingRedisLink;
use Amp\Redis\Connection\SocketRedisConnector;
use function Amp\Redis\createRedisClient;

$redis = new Redis(createRedisClient('redis://'));

$redis->set('foo', '21');
$result = $redis->increment('foo', 21);

\var_dump($result); // int(42)
```

## Security

If you discover any security related issues, please use the private security issue reporter instead of using the public
issue tracker.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
