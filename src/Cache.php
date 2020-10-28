<?php

namespace Amp\Redis;

use Amp\Cache\Cache as CacheInterface;
use Amp\Cache\CacheException;

final class Cache implements CacheInterface
{
    /** @var Redis */
    private Redis $redis;

    /**
     * @param Redis $redis
     */
    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    /** @inheritdoc */
    public function get(string $key): ?string
    {
        try {
            return $this->redis->get($key);
        } catch (RedisException $e) {
            throw new CacheException("Fetching '${key}' from cache failed", 0, $e);
        }
    }

    /** @inheritdoc */
    public function set(string $key, string $value, int $ttl = null): void
    {
        if ($ttl !== null && $ttl < 0) {
            throw new \Error('Invalid TTL: ' . $ttl);
        }

        if ($ttl === 0) {
            return; // expires immediately
        }

        try {
            $options = new SetOptions;

            if ($ttl !== null) {
                $options = $options->withTtl($ttl);
            }

            $this->redis->set($key, $value, $options);
        } catch (RedisException $e) {
            throw new CacheException("Storing '{$key}' to cache failed", 0, $e);
        }
    }

    /** @inheritdoc */
    public function delete(string $key): bool
    {
        try {
            return $this->redis->delete($key);
        } catch (RedisException $e) {
            throw new CacheException("Deleting '{$key}' from cache failed", 0, $e);
        }
    }
}
