<?php /** @noinspection ForgottenDebugOutputInspection */

require __DIR__ . '/../vendor/autoload.php';

Amp\Loop::run(static function () {
    $client = new Amp\Redis\Redis(new Amp\Redis\RemoteExecutor('tcp://localhost:6379'));

    yield $client->set('foo', '21');
    $result = yield $client->increment('foo', 21);

    \var_dump($result); // int(42)
});
