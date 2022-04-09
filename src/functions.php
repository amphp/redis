<?php

namespace Amp\Redis;

use Amp\Cancellation;
use Amp\Socket\ConnectContext;
use Revolt\EventLoop;

function toFloat(mixed $response): ?float
{
    if ($response === null) {
        return null;
    }

    return (float) $response;
}

function toBool(mixed $response): ?bool
{
    if ($response === null) {
        return null;
    }

    return (bool) $response;
}

function toMap(?array $values): ?array
{
    if ($values === null) {
        return null;
    }

    $size = \count($values);
    $result = [];

    for ($i = 0; $i < $size; $i += 2) {
        $result[$values[$i]] = $values[$i + 1];
    }

    return $result;
}

function toNull(mixed $response): void
{
    // nothing to do
}

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

    return $map[$driver] ??= new RedisSocketConnector();
}

/**
 * @throws RedisException
 */
function connect(
    Config $config,
    ?ConnectContext $context = null,
    ?Cancellation $cancellation = null,
): RespSocket {
    return redisConnector()->connect($config, $context, $cancellation);
}
