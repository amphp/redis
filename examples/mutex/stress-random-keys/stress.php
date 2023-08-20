<?php declare(strict_types=1);

require __DIR__ . '/../../../vendor/autoload.php';

use Amp\Redis\Connection\ChannelLink;
use Amp\Redis\Connection\SocketChannelFactory;
use Amp\Redis\RedisClient;
use Amp\Redis\RedisConfig;

$config = RedisConfig::fromUri('redis://');

$redis = new Amp\Redis\Redis(new RedisClient(new ChannelLink($config, new SocketChannelFactory($config))));
$mutex = new Amp\Redis\Sync\RedisMutex(new RedisClient(new ChannelLink($config, new SocketChannelFactory($config))));

for ($i = 0; $i < 100; $i++) {
    $lock = $mutex->acquire('test' . random_int(0, 49));
    $lock->release();

    if ($i % 10 === 0) {
        print '.';
    }
}

$redis->increment('attempts', $mutex->getNumberOfAttempts());
