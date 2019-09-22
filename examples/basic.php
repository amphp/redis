<?php /** @noinspection ForgottenDebugOutputInspection */

require __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\Config;
use Amp\Redis\Redis;
use Amp\Redis\RemoteExecutor;

Amp\Loop::run(static function () {
    $redis = new Redis(new RemoteExecutor(Config::fromUri('redis://')));

    yield $redis->set('foo', '21');
    $result = yield $redis->increment('foo', 21);

    \var_dump($result); // int(42)
});
