<?php

namespace Amp\Redis;

use Amp\Socket;
use Amp\Socket\ConnectContext;

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
 * @throws RedisException
 */
function connect(Config $config, ?Socket\SocketConnector $connector = null): RespSocket
{
    try {
        $connectContext = (new ConnectContext)->withConnectTimeout($config->getTimeout());
        $resp = new RespSocket(
            ($connector ?? Socket\socketConnector())->connect($config->getConnectUri(), $connectContext)
        );
    } catch (Socket\SocketException $e) {
        throw new SocketException(
            'Failed to connect to redis instance (' . $config->getConnectUri() . ')',
            0,
            $e
        );
    }

    $readsNeeded = 0;

    if ($config->hasPassword()) {
        $readsNeeded++;
        $resp->write('AUTH', $config->getPassword());
    }

    if ($config->getDatabase() !== 0) {
        $readsNeeded++;
        $resp->write('SELECT', (string) $config->getDatabase());
    }

    for ($i = 0; $i < $readsNeeded; $i++) {
        if ([$response] = $resp->read()) {
            if ($response instanceof \Throwable) {
                throw $response;
            }
        } else {
            throw new RedisException('Failed to connect to redis instance (' . $config->getConnectUri() . ')');
        }
    }

    return $resp;
}
