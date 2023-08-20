<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\Connection\ChannelLink;
use Amp\Redis\Connection\SocketChannelFactory;
use Amp\Redis\Redis;
use Amp\Redis\RedisClient;
use Amp\Redis\RedisConfig;

$config = RedisConfig::fromUri('redis://');
$redis = new Redis(new RedisClient(new ChannelLink($config, new SocketChannelFactory($config))));

$redis->set('foo', '21');
$result = $redis->increment('foo', 21);

var_dump($result); // int(42)
