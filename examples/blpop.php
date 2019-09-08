<?php

require __DIR__ . '/../vendor/autoload.php';

Amp\Loop::run(static function () {
    $pushClient = new Amp\Redis\Redis(new Amp\Redis\RemoteExecutor('tcp://localhost:6379'));
    $pushClient->getList('foobar-list')->popHeadBlocking()->onResolve(static function (?\Throwable $error, $value) {
        if ($error) {
            print 'Error: ' . $error->getMessage() . PHP_EOL;
        } else {
            print 'Value: ' . \var_export($value, true) . PHP_EOL;
        }

        Amp\Loop::stop();
    });

    $client = new Amp\Redis\Redis(new Amp\Redis\RemoteExecutor('tcp://localhost:6379'));

    print 'Pushing value…' . PHP_EOL;
    yield $client->getList('foobar-list')->pushHead('42');
    print 'Value pushed.' . PHP_EOL;
});
