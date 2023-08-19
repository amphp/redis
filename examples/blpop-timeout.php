<?php declare(strict_types=1);

use Revolt\EventLoop;

require __DIR__ . '/../vendor/autoload.php';

$client = new Amp\Redis\Redis(new Amp\Redis\SocketRedisClient('redis://'));

$client->delete('foobar-list');

EventLoop::unreference(EventLoop::repeat(1, static function (): void {
    print 'Waiting for blpop…' . PHP_EOL;
}));

$value = $client->getList('foobar-list')->popHeadBlocking(5);

var_dump($value);
