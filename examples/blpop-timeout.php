<?php declare(strict_types=1);

use Amp\Redis\Connection\ChannelLink;
use Amp\Redis\Connection\SocketChannelFactory;
use Amp\Redis\Redis;
use Amp\Redis\RedisClient;
use Amp\Redis\RedisConfig;
use Revolt\EventLoop;

require __DIR__ . '/../vendor/autoload.php';

$config = RedisConfig::fromUri('redis://');
$client = new Redis(new RedisClient(new ChannelLink($config, new SocketChannelFactory($config))));

$client->delete('foobar-list');

EventLoop::unreference(EventLoop::repeat(1, static function (): void {
    print 'Waiting for blpop…' . PHP_EOL;
}));

$value = $client->getList('foobar-list')->popHeadBlocking(5);

var_dump($value);
