<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\Redis;
use Amp\Redis\RedisConfig;
use Amp\Redis\SocketRedisClient;

$redis = new Redis(new SocketRedisClient('redis://'));

$redis->set('foo', '21');
$result = $redis->increment('foo', 21);

var_dump($result); // int(42)
