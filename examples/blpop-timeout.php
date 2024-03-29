<?php declare(strict_types=1);

use Revolt\EventLoop;
use function Amp\Redis\createRedisClient;

require __DIR__ . '/../vendor/autoload.php';

$client = createRedisClient('redis://');

$client->delete('foobar-list');

EventLoop::unreference(EventLoop::repeat(1, static function (): void {
    print 'Waiting for blpop…' . PHP_EOL;
}));

$value = $client->getList('foobar-list')->popHeadBlocking(5);

var_dump($value);
