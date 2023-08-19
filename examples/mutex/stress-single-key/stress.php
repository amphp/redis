<?php declare(strict_types=1);

require __DIR__ . '/../../../vendor/autoload.php';

use Amp\Redis\RedisConfig;
use Amp\Redis\SocketRedisClientFactory;

$clientFactory = new SocketRedisClientFactory('redis://localhost');

$redis = new Amp\Redis\Redis($clientFactory->createRedisClient());
$mutex = new Amp\Redis\Sync\RedisMutex($clientFactory->createRedisClient());

for ($i = 0; $i < 100; $i++) {
    $lock = $mutex->acquire('test');

    $count = $redis->get('foo');
    $redis->set('foo', (string) ++$count);

    $lock->release();

    if ($i % 10 === 0) {
        print '.';
    }
}

$redis->increment('attempts', $mutex->getNumberOfAttempts());
