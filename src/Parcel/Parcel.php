<?php

namespace Amp\Redis\Parcel;

use Amp\Parallel\Sync\BuiltInSerializer;
use Amp\Parallel\Sync\Parcel as ParallelParcel;
use Amp\Parallel\Sync\ParcelException;
use Amp\Parallel\Sync\Serializer;
use Amp\Promise;
use Amp\Redis\Mutex\Mutex as RedisMutex;
use Amp\Redis\QueryExecutorFactory;
use Amp\Redis\Redis;
use Amp\Success;
use Amp\Sync\Lock;
use function Amp\call;

final class Parcel implements ParallelParcel
{
    private const KEY_PREFIX = 'parcel:';

    /** @var string */
    private $key;

    /** @var Redis|null */
    private $redis;

    /** @var RedisMutex */
    private $mutex;

    /** @var int */
    private $initiator = 0;

    /** @var Serializer */
    private $serializer;

    /**
     * @param QueryExecutorFactory $executorFactory
     * @param RedisMutex           $mutex
     * @param string               $key
     * @param mixed                $value Initial value for the parcel, must be serializable.
     * @param Serializer|null      $serializer
     *
     * @return Promise<self>
     */
    public static function create(
        QueryExecutorFactory $executorFactory,
        RedisMutex $mutex,
        string $key,
        $value,
        ?Serializer $serializer = null
    ): Promise {
        return (new self($executorFactory, $mutex, $key, $serializer))->init($value);
    }

    /**
     * @param QueryExecutorFactory $executorFactory
     * @param RedisMutex           $mutex
     * @param string               $key
     * @param Serializer|null      $serializer
     *
     * @return Promise<self>
     */
    public static function use(
        QueryExecutorFactory $executorFactory,
        RedisMutex $mutex,
        string $key,
        ?Serializer $serializer = null
    ): Promise {
        return (new self($executorFactory, $mutex, $key, $serializer))->open();
    }

    private function __construct(
        QueryExecutorFactory $executorFactory,
        RedisMutex $mutex,
        string $key,
        ?Serializer $serializer = null
    ) {
        $this->redis = new Redis($executorFactory->createQueryExecutor());
        $this->mutex = $mutex;
        $this->key = $key;
        $this->serializer = $serializer ?? new BuiltInSerializer;
    }

    private function init($value): Promise
    {
        return call(function () use ($value): \Generator {
            $value = $this->serializer->serialize($value);

            $lock = yield $this->mutex->acquire($this->key);
            \assert($lock instanceof Lock);

            try {
                if (yield $this->redis->get(self::KEY_PREFIX . $this->key)) {
                    throw new ParcelException('Could not initialized parcel: key already exists');
                }

                yield $this->redis->set(self::KEY_PREFIX . $this->key, $value);

                $this->initiator = \getmypid();
            } finally {
                $lock->release();
            }

            return $this;
        });
    }

    private function open(): Promise
    {
        return call(function () {
            if (!yield $this->redis->get(self::KEY_PREFIX . $this->key)) {
                throw new ParcelException('Could not open parcel: key not found');
            }

            return $this;
        });
    }

    public function __destruct()
    {
        Promise\rethrow($this->free());
    }

    public function unwrap(): Promise
    {
        if ($this->redis === null) {
            throw new \Error('The parcel has been freed');
        }

        return call(function (): \Generator {
            $value = yield $this->redis->get(self::KEY_PREFIX . $this->key);
            return $this->serializer->unserialize($value);
        });
    }

    public function synchronized(callable $callback): Promise
    {
        if ($this->redis === null) {
            throw new \Error('The parcel has been freed');
        }

        return call(function () use ($callback): \Generator {
            $lock = yield $this->mutex->acquire($this->key);
            \assert($lock instanceof Lock);

            try {
                $result = yield call($callback, yield $this->unwrap());

                if ($result !== null && $this->redis !== null) {
                    yield $this->redis->set(self::KEY_PREFIX . $this->key, $this->serializer->serialize($result));
                }
            } finally {
                $lock->release();
            }

            return $result;
        });
    }

    /**
     * @return Promise<null>
     */
    private function free(): Promise
    {
        if ($this->redis === null) {
            return new Success;
        }

        $redis = $this->redis;
        $this->redis = null;

        if ($this->initiator === 0 || $this->initiator !== \getmypid()) {
            return new Success;
        }

        return call(function () use ($redis): \Generator {
            $lock = yield $this->mutex->acquire($this->key);
            \assert($lock instanceof Lock);

            try {
                $redis->delete(self::KEY_PREFIX . $this->key);
            } finally {
                $lock->release();
            }
        });
    }
}
