<?php

require __DIR__ . '/../../../vendor/autoload.php';

use Amp\Redis\Config;
use Amp\Redis\RemoteExecutorFactory;

$executorFactory = new RemoteExecutorFactory(Config::fromUri('redis://'));

$redis = new Amp\Redis\Redis($executorFactory->createQueryExecutor());
$mutex = new Amp\Redis\Mutex\Mutex($executorFactory);

for ($i = 0; $i < 100; $i++) {
    $lock = $mutex->acquire('test' . random_int(0, 49));
    $lock->release();

    if ($i % 10 === 0) {
        print '.';
    }
}

$redis->increment('attempts', $mutex->getNumberOfAttempts());
