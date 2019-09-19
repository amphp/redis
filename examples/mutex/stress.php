<?php

require __DIR__ . '/../../vendor/autoload.php';

use Amp\Loop;
use Amp\Redis\Config;
use Amp\Redis\RemoteExecutorFactory;

Loop::run(static function () {
    $executorFactory = new RemoteExecutorFactory(Config::fromUri('redis://'));

    $redis = new Amp\Redis\Redis($executorFactory->createQueryExecutor());
    $mutex = new Amp\Redis\Mutex\Mutex($executorFactory);

    for ($i = 0; $i < 100; $i++) {
        $token = \base64_encode(\random_bytes(16));
        yield $mutex->lock('test', $token);

        $count = yield $redis->get('foo');
        yield $redis->set('foo', ++$count);

        yield $mutex->unlock('test', $token);

        if ($i % 10 === 0) {
            print '.';
        }
    }

    yield $redis->increment('attempts', $mutex->getNumberOfAttempts());
});
