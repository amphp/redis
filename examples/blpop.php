<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\Connection\ChannelLink;
use Amp\Redis\Connection\SocketChannelFactory;
use Amp\Redis\Redis;
use Amp\Redis\RedisClient;
use Amp\Redis\RedisConfig;
use function Amp\async;

$future = async(function (): void {
    $config = RedisConfig::fromUri('redis://');
    $popClient = new Redis(new RedisClient(new ChannelLink($config, new SocketChannelFactory($config))));

    try {
        $value = $popClient->getList('foobar-list')->popHeadBlocking();
        print 'Value: ' . var_export($value, true) . PHP_EOL;
    } catch (\Throwable $error) {
        print 'Error: ' . $error->getMessage() . PHP_EOL;
    }
});

$config = RedisConfig::fromUri('redis://');
$pushClient = new Redis(new RedisClient(new ChannelLink($config, new SocketChannelFactory($config))));

print 'Pushing value…' . PHP_EOL;
$pushClient->getList('foobar-list')->pushHead('42');
print 'Value pushed.' . PHP_EOL;

$future->await();
