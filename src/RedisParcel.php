<?php

namespace Amp\Redis;

use Amp\Redis\Mutex\RedisMutex;
use Amp\Serialization\NativeSerializer;
use Amp\Serialization\Serializer;
use Amp\Sync\Parcel;
use Amp\Sync\ParcelException;
use Revolt\EventLoop;

final class RedisParcel implements Parcel
{
    private string $key;

    private Redis $redis;

    private RedisMutex $mutex;

    private int $initiator = 0;

    private Serializer $serializer;

    public static function create(
        RedisMutex $mutex,
        string $key,
        mixed $value,
        ?Serializer $serializer = null,
    ): self {
        return (new self($mutex, $key, $serializer))->init($value);
    }

    public static function use(
        RedisMutex $mutex,
        string $key,
        ?Serializer $serializer = null,
    ): self {
        return (new self($mutex, $key, $serializer))->open();
    }

    private function __construct(
        RedisMutex $mutex,
        string $key,
        ?Serializer $serializer = null
    ) {
        $this->redis = new Redis($mutex->getQueryExecutor());
        $this->mutex = $mutex;
        $this->key = $key;
        $this->serializer = $serializer ?? new NativeSerializer();
    }

    private function init(mixed $value): self
    {
        $value = $this->serializer->serialize($value);

        $lock = $this->mutex->acquire($this->key);

        try {
            if ($this->redis->get($this->key)) {
                throw new ParcelException('Could not initialized parcel: key already exists');
            }

            $this->redis->set($this->key, $value);

            $this->initiator = \getmypid();
        } finally {
            $lock->release();
        }

        return $this;
    }

    private function open(): self
    {
        if (!$this->redis->get($this->key)) {
            throw new ParcelException('Could not open parcel: key not found');
        }

        return $this;
    }

    public function __destruct()
    {
        $this->free();
    }

    public function getQueryExecutor(): QueryExecutor
    {
        return $this->mutex->getQueryExecutor();
    }

    public function unwrap(): mixed
    {
        $value = $this->redis->get($this->key) ?? throw new ParcelException('Could not unwrap parcel: key not found');
        return $this->serializer->unserialize($value);
    }

    public function synchronized(\Closure $closure): mixed
    {
        $lock = $this->mutex->acquire($this->key);

        try {
            $result = $closure($this->unwrap());

            if ($result !== null) {
                $this->redis->set($this->key, $this->serializer->serialize($result));
            }
        } finally {
            $lock->release();
        }

        return $result;
    }

    private function free(): void
    {
        if ($this->initiator === 0 || $this->initiator !== \getmypid()) {
            return;
        }

        $redis = $this->redis;
        $mutex = $this->mutex;
        $key = $this->key;
        EventLoop::queue(static function () use ($redis, $mutex, $key): void {
            $lock = $mutex->acquire($key);

            try {
                $redis->delete($key);
            } finally {
                $lock->release();
            }
        });
    }
}
