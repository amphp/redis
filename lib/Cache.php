<?php

namespace Amp\Redis;

use Amp\Cache\CacheException;
use Amp\Promise;
use function Amp\call;

class Cache implements \Amp\Cache\Cache
{
    /** @var Client */
    private $client;

    /**
     * @param \Amp\Redis\Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /** @inheritdoc */
    public function get(string $key): Promise
    {
        return call(function () use ($key) {
            try {
                return yield $this->client->get($key);
            } catch (RedisException $e) {
                throw new CacheException("Fetching from cache failed", 0, $e);
            }
        });
    }

    /** @inheritdoc */
    public function set(string $key, string $value, int $ttl = null): Promise
    {
        if ($ttl && $ttl < 0) {
            throw new \Error("Invalid TTL: {$ttl}");
        }

        return call(function () use ($key, $value, $ttl) {
            try {
                return yield $this->client->set($key, $value, $ttl === null ? 0 : $ttl);
            } catch (RedisException $e) {
                throw new CacheException("Storing to cache failed", 0, $e);
            }
        });
    }

    /** @inheritdoc */
    public function delete(string $key): Promise
    {
        return call(function () use ($key) {
            try {
                return yield $this->client->del($key);
            } catch (RedisException $e) {
                throw new CacheException("Deleting from cache failed", 0, $e);
            }
        });
    }
}
