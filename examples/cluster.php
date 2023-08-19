<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\Cluster\ClusterExecutor;
use Amp\Redis\Redis;
use Amp\Redis\RedisConfig;

$redis = new Redis(new ClusterExecutor([
    RedisConfig::fromUri('redis://localhost:6001'),
    RedisConfig::fromUri('redis://localhost:6002'),
]));

$redis->set('foo', '21');
$result = $redis->increment('foo', 21);

$result = $redis->getMultiple('foo', 'bar');

//foreach ($redis->scan('foo') as $value) {
//    var_dump($value);
//}

var_dump($result); // int(42)
