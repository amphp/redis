<?php

namespace Amp\Redis;

use Amp\Cache\Cache as CacheInterface;
use Amp\Cache\CacheException;
use Amp\Promise;
use Amp\Success;
use function Amp\call;

final class Cache implements CacheInterface
{
    /** @var Redis */
    private $redis;

    /**
     * @param Redis $redis
     */
    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    /** @inheritdoc */
    public function get(string $key): Promise
    {
        return call(function () use ($key) {
            try {
                return yield $this->redis->get($key);
            } catch (RedisException $e) {
                throw new CacheException("Fetching '{$key}' from cache failed", 0, $e);
            }
        });
    }

    /** @inheritdoc */
    public function set(string $key, string $value, int $ttl = null): Promise
    {
        if ($ttl !== null && $ttl < 0) {
            throw new \Error('Invalid TTL: ' . $ttl);
        }

        if ($ttl === 0) {
            return new Success; // expires immediately
        }

        return call(function () use ($key, $value, $ttl) {
            try {
                $options = new SetOptions;

                if ($ttl !== null) {
                    $options = $options->withTtl($ttl);
                }

                return yield $this->redis->set($key, $value, $options);
            } catch (RedisException $e) {
                throw new CacheException("Storing '{$key}' to cache failed", 0, $e);
            }
        });
    }

    /** @inheritdoc */
    public function delete(string $key): Promise
    {
        return call(function () use ($key) {
            try {
                return yield $this->redis->delete($key);
            } catch (RedisException $e) {
                throw new CacheException("Deleting '{$key}' from cache failed", 0, $e);
            }
        });
    }
}
