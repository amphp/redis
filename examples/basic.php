<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use function Amp\Redis\createRedisClient;

$redis = createRedisClient('redis://');

$redis->set('foo', '21');
$result = $redis->increment('foo', 21);

var_dump($result); // int(42)
