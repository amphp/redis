<?php

require __DIR__ . '/../../vendor/autoload.php';

use Amp\Loop;
use Amp\Redis\Config;
use Amp\Redis\RemoteExecutorFactory;

Loop::run(static function () {
    $executorFactory = new RemoteExecutorFactory(Config::fromUri('redis://'));

    $redis = new Amp\Redis\Redis($executorFactory->createQueryExecutor());

    $count = yield $redis->get('foo');
    $attempts = yield $redis->get('attempts');

    print PHP_EOL;
    print 'Result: ' . $count . PHP_EOL;
    print 'Expect: ' . 5000 . PHP_EOL;
    print 'Attempts: ' . $attempts . PHP_EOL;
});
