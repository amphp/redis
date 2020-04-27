<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\Promise;

final class RedisList
{
    /** @var QueryExecutor */
    private $queryExecutor;
    /** @var string */
    private $key;

    public function __construct(QueryExecutor $queryExecutor, string $key)
    {
        $this->queryExecutor = $queryExecutor;
        $this->key = $key;
    }

    /**
     * @param string $index
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/lindex
     */
    public function get(string $index): Promise
    {
        return $this->queryExecutor->execute(['lindex', $this->key, $index]);
    }

    /**
     * @param string $pivot
     * @param string $value
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/linsert
     */
    public function insertBefore(string $pivot, string $value): Promise
    {
        return $this->queryExecutor->execute(['linsert', $this->key, 'BEFORE', $pivot, $value]);
    }

    /**
     * @param string $pivot
     * @param string $value
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/linsert
     */
    public function insertAfter(string $pivot, string $value): Promise
    {
        return $this->queryExecutor->execute(['linsert', $this->key, 'AFTER', $pivot, $value]);
    }

    /**
     * @return Promise<int>
     *
     * @link https://redis.io/commands/llen
     */
    public function getSize(): Promise
    {
        return $this->queryExecutor->execute(['llen', $this->key]);
    }

    /**
     * @param string $value
     * @param string ...$values
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/lpush
     */
    public function pushHead(string $value, string ...$values): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['lpush', $this->key, $value], $values));
    }

    /**
     * @param string $value
     * @param string ...$values
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/lpushx
     */
    public function pushHeadIfExists(string $value, string ...$values): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['lpushx', $this->key, $value], $values));
    }

    /**
     * @param string $value
     * @param string ...$values
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/rpush
     */
    public function pushTail(string $value, string ...$values): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['rpush', $this->key, $value], $values));
    }

    /**
     * @param string $value
     * @param string ...$values
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/rpushx
     */
    public function pushTailIfExists(string $value, string ...$values): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['rpushx', $this->key, $value], $values));
    }

    /**
     * @return Promise<string>
     *
     * @link https://redis.io/commands/lpop
     */
    public function popHead(): Promise
    {
        return $this->queryExecutor->execute(['lpop', $this->key]);
    }

    /**
     * @param int $timeout
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/blpop
     */
    public function popHeadBlocking(int $timeout = 0): Promise
    {
        return $this->queryExecutor->execute(['blpop', $this->key, $timeout], static function ($response) {
            return $response[1] ?? null;
        });
    }

    /**
     * @return Promise<string>
     *
     * @link https://redis.io/commands/rpop
     */
    public function popTail(): Promise
    {
        return $this->queryExecutor->execute(['rpop', $this->key]);
    }

    /**
     * @param int $timeout
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/brpop
     */
    public function popTailBlocking(int $timeout = 0): Promise
    {
        return $this->queryExecutor->execute(['brpop', $this->key, $timeout], static function ($response) {
            return $response[1];
        });
    }

    /**
     * @param string $destination
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/rpoplpush
     */
    public function popTailPushHead(string $destination): Promise
    {
        return $this->queryExecutor->execute(['rpoplpush', $this->key, $destination]);
    }

    /**
     * @param string $destination
     * @param int    $timeout
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/brpoplpush
     */
    public function popTailPushHeadBlocking(string $destination, int $timeout = 0): Promise
    {
        return $this->queryExecutor->execute(['brpoplpush', $this->key, $destination, $timeout]);
    }

    /**
     * @param int $start
     * @param int $end
     *
     * @return Promise<array>
     *
     * @link https://redis.io/commands/lrange
     */
    public function getRange(int $start = 0, int $end = -1): Promise
    {
        return $this->queryExecutor->execute(['lrange', $this->key, $start, $end]);
    }

    /**
     * @param string $value
     * @param int    $count
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/lrem
     */
    public function remove(string $value, int $count = 0): Promise
    {
        return $this->queryExecutor->execute(['lrem', $this->key, $count, $value]);
    }

    /**
     * @param int    $index
     * @param string $value
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/lset
     */
    public function set(int $index, string $value): Promise
    {
        return $this->queryExecutor->execute(['lset', $this->key, $index, $value], toNull);
    }

    /**
     * @param int $start
     * @param int $stop
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/ltrim
     */
    public function trim(int $start = 0, int $stop = -1): Promise
    {
        return $this->queryExecutor->execute(['ltrim', $this->key, $start, $stop], toNull);
    }

    /**
     * @param SortOptions $sort
     *
     * @return Promise<array>
     *
     * @link https://redis.io/commands/sort
     */
    public function sort(?SortOptions $sort = null): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['SORT', $this->key], ($sort ?? new SortOptions)->toQuery()));
    }
}
