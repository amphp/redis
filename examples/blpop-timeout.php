<?php /** @noinspection ForgottenDebugOutputInspection */

require __DIR__ . '/../vendor/autoload.php';

Amp\Loop::run(static function () {
    $client = new Amp\Redis\Redis(new Amp\Redis\RemoteExecutor('tcp://localhost:6379'));

    yield $client->delete('foobar-list');

    Amp\Loop::unreference(Amp\Loop::repeat(1000, static function () {
        print 'Waiting for blpop…' . PHP_EOL;
    }, 1000));

    $value = yield $client->getList('foobar-list')->popHeadBlocking(5);

    \var_dump($value);
});
