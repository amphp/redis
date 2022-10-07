<?php

namespace Amp\Redis;

use Amp\Promise;
use Amp\Socket;
use Amp\Socket\ConnectContext;
use function Amp\call;

const toFloat = __NAMESPACE__ . '\toFloat';
const toBool = __NAMESPACE__ . '\toBool';
const toNull = __NAMESPACE__ . '\toNull';
const toMap = __NAMESPACE__ . '\toMap';
const toFloatMap = __NAMESPACE__ . '\toFloatMap';

function toFloat($response): ?float
{
    if ($response === null) {
        return null;
    }

    return (float) $response;
}

function toBool($response): ?bool
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

function toFloatMap(?array $values): ?array
{
    if ($values === null) {
        return null;
    }

    $size = \count($values);
    $result = [];

    for ($i = 0; $i < $size; $i += 2) {
        $result[$values[$i]] = (float)$values[$i + 1];
    }

    return $result;
}

function toNull($response): void
{
    // nothing to do
}

/**
 * @param Config $config
 * @param Socket\Connector|null $connector
 *
 * @return Promise<RespSocket>
 *
 * @throws RedisException
 */
function connect(Config $config, ?Socket\Connector $connector = null): Promise
{
    return call(static function () use ($config, $connector) {
        try {
            $connectContext = (new ConnectContext)->withConnectTimeout($config->getTimeout());
            $resp = new RespSocket(
                yield ($connector ?? Socket\connector())->connect($config->getConnectUri(), $connectContext)
            );
        } catch (Socket\SocketException $e) {
            throw new SocketException(
                'Failed to connect to redis instance (' . $config->getConnectUri() . ')',
                0,
                $e
            );
        }

        $promises = [];

        if ($config->hasPassword()) {
            // pipeline, don't await
            $promises[] = $resp->write('AUTH', $config->getPassword());
        }

        if ($config->getDatabase() !== 0) {
            // pipeline, don't await
            $promises[] = $resp->write('SELECT', $config->getDatabase());
        }

        foreach ($promises as $promise) {
            yield $promise;

            if ([$response] = yield $resp->read()) {
                if ($response instanceof \Throwable) {
                    throw $response;
                }
            } else {
                throw new RedisException('Failed to connect to redis instance (' . $config->getConnectUri() . ')');
            }
        }

        return $resp;
    });
}
