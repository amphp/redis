<?php declare(strict_types=1);

require __DIR__ . '/../../../vendor/autoload.php';

use Amp\Redis\RedisConfig;
use Amp\Redis\RemoteExecutorFactory;

$executorFactory = new RemoteExecutorFactory(RedisConfig::fromUri('redis://'));

$redis = new Amp\Redis\Redis($executorFactory->createQueryExecutor());
$mutex = new Amp\Redis\Sync\RedisMutex($executorFactory->createQueryExecutor());

for ($i = 0; $i < 100; $i++) {
    $lock = $mutex->acquire('test' . random_int(0, 49));
    $lock->release();

    if ($i % 10 === 0) {
        print '.';
    }
}

$redis->increment('attempts', $mutex->getNumberOfAttempts());
