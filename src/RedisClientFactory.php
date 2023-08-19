<?php declare(strict_types=1);

namespace Amp\Redis;

interface RedisClientFactory
{
    /**
     * @return RedisClient New RedisClient instance.
     */
    public function createRedisClient(): RedisClient;
}
