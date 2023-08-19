<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Redis\Connection\RedisConnector;

final class SocketRedisClientFactory implements RedisClientFactory
{
    public function __construct(
        private readonly RedisConfig|string $config,
        private readonly ?RedisConnector $connector = null,
    ) {
    }

    public function createRedisClient(): RedisClient
    {
        return new SocketRedisClient($this->config, $this->connector);
    }
}
