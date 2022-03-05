<?php

namespace Amp\Redis;

use Amp\Cache\Cache as CacheInterface;
use Amp\Cache\CacheException;
use Amp\Serialization\NativeSerializer;
use Amp\Serialization\Serializer;

final class Cache implements CacheInterface
{
    /** @var Redis */
    private readonly Redis $redis;

    private readonly Serializer $serializer;

    public function __construct(Redis $redis, ?Serializer $serializer = null)
    {
        $this->redis = $redis;
        $this->serializer = $serializer ?? new NativeSerializer();
    }

    public function get(string $key): mixed
    {
        try {
            $data = $this->redis->get($key);
            if ($data === null) {
                return null;
            }

            return $this->serializer->unserialize($data);
        } catch (RedisException $e) {
            throw new CacheException("Fetching '${key}' from cache failed", 0, $e);
        }
    }

    public function set(string $key, mixed $value, int $ttl = null): void
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

            $this->redis->set($key, $this->serializer->serialize($value), $options);
        } catch (RedisException $e) {
            throw new CacheException("Storing '{$key}' to cache failed", 0, $e);
        }
    }

    public function delete(string $key): bool
    {
        try {
            return $this->redis->delete($key);
        } catch (RedisException $e) {
            throw new CacheException("Deleting '{$key}' from cache failed", 0, $e);
        }
    }
}
