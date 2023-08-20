<?php declare(strict_types=1);

require __DIR__ . '/../../../vendor/autoload.php';

use function Amp\Redis\createRedisClient;

$redis = new \Amp\Redis\RedisClient(createRedisClient('redis://'));
$mutex = new Amp\Redis\Sync\RedisMutex(createRedisClient('redis://'));

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
