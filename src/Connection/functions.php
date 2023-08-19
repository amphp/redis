<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Revolt\EventLoop;

/**
 * Set or access the global RedisConnector instance.
 */
function redisConnector(?RedisConnector $connector = null): RedisConnector
{
    static $map;
    $map ??= new \WeakMap();
    $driver = EventLoop::getDriver();

    if ($connector) {
        return $map[$driver] = $connector;
    }

    return $map[$driver] ??= new SocketRedisConnector();
}
