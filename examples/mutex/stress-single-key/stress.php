<?php declare(strict_types=1);

require __DIR__ . '/../../../vendor/autoload.php';

use Amp\Redis\Connection\ChannelRedisLink;
use Amp\Redis\Connection\SocketRedisChannelFactory;
use Amp\Redis\RedisClient;
use Amp\Redis\RedisConfig;

$config = RedisConfig::fromUri('redis://');

$redis = new Amp\Redis\Redis(new RedisClient(new ChannelRedisLink($config, new SocketRedisChannelFactory($config))));
$mutex = new Amp\Redis\Sync\RedisMutex(new RedisClient(new ChannelRedisLink($config, new SocketRedisChannelFactory($config))));

for ($i = 0; $i < 100; $i++) {
    $lock = $mutex->acquire('test');

    $count = $redis->get('foo');
    $redis->set('foo', (string)++$count);

    $lock->release();

    if ($i % 10 === 0) {
        print '.';
    }
}

$redis->increment('attempts', $mutex->getNumberOfAttempts());
