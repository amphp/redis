<?php declare(strict_types=1);

require __DIR__ . '/../../../vendor/autoload.php';

use Amp\Redis\RedisConfig;
use Amp\Redis\RemoteExecutorFactory;

$executorFactory = new RemoteExecutorFactory(RedisConfig::fromUri('redis://localhost'));

$redis = new Amp\Redis\Redis($executorFactory->createQueryExecutor());
$mutex = new Amp\Redis\Mutex\RedisMutex($executorFactory);

for ($i = 0; $i < 100; $i++) {
    $lock = $mutex->acquire('test');

    $count = $redis->get('foo');
    $redis->set('foo', ++$count);

    $lock->release();

    if ($i % 10 === 0) {
        print '.';
    }
}

$redis->increment('attempts', $mutex->getNumberOfAttempts());
