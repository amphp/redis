<?php declare(strict_types=1);
/** @noinspection DuplicatedCode */

namespace Amp\Redis;

final class RedisList
{
    public function __construct(
        private readonly QueryExecutor $queryExecutor,
        private readonly string $key,
    ) {
    }

    /**
     * @link https://redis.io/commands/lindex
     */
    public function get(string $index): string
    {
        return $this->queryExecutor->execute(['lindex', $this->key, $index]);
    }

    /**
     * @link https://redis.io/commands/linsert
     */
    public function insertBefore(string $pivot, string $value): int
    {
        return $this->queryExecutor->execute(['linsert', $this->key, 'BEFORE', $pivot, $value]);
    }

    /**
     * @link https://redis.io/commands/linsert
     */
    public function insertAfter(string $pivot, string $value): int
    {
        return $this->queryExecutor->execute(['linsert', $this->key, 'AFTER', $pivot, $value]);
    }

    /**
     *
     * @link https://redis.io/commands/llen
     */
    public function getSize(): int
    {
        return $this->queryExecutor->execute(['llen', $this->key]);
    }

    /**
     * @link https://redis.io/commands/lpush
     */
    public function pushHead(string $value, string ...$values): int
    {
        return $this->queryExecutor->execute(\array_merge(['lpush', $this->key, $value], $values));
    }

    /**
     * @link https://redis.io/commands/lpushx
     */
    public function pushHeadIfExists(string $value, string ...$values): int
    {
        return $this->queryExecutor->execute(\array_merge(['lpushx', $this->key, $value], $values));
    }

    /**
     * @link https://redis.io/commands/rpush
     */
    public function pushTail(string $value, string ...$values): int
    {
        return $this->queryExecutor->execute(\array_merge(['rpush', $this->key, $value], $values));
    }

    /**
     * @link https://redis.io/commands/rpushx
     */
    public function pushTailIfExists(string $value, string ...$values): int
    {
        return $this->queryExecutor->execute(\array_merge(['rpushx', $this->key, $value], $values));
    }

    /**
     *
     * @link https://redis.io/commands/lpop
     */
    public function popHead(): ?string
    {
        return $this->queryExecutor->execute(['lpop', $this->key]);
    }

    /**
     * @link https://redis.io/commands/blpop
     */
    public function popHeadBlocking(int $timeout = 0): ?string
    {
        return $this->queryExecutor->execute(['blpop', $this->key, $timeout], static function ($response) {
            return $response[1] ?? null;
        });
    }

    /**
     * @link https://redis.io/commands/rpop
     */
    public function popTail(): ?string
    {
        return $this->queryExecutor->execute(['rpop', $this->key]);
    }

    /**
     * @link https://redis.io/commands/brpop
     */
    public function popTailBlocking(int $timeout = 0): ?string
    {
        return $this->queryExecutor->execute(['brpop', $this->key, $timeout], static function ($response) {
            return $response[1] ?? null;
        });
    }

    /**
     * @link https://redis.io/commands/rpoplpush
     */
    public function popTailPushHead(string $destination): string
    {
        return $this->queryExecutor->execute(['rpoplpush', $this->key, $destination]);
    }

    /**
     * @link https://redis.io/commands/brpoplpush
     */
    public function popTailPushHeadBlocking(string $destination, int $timeout = 0): ?string
    {
        return $this->queryExecutor->execute(['brpoplpush', $this->key, $destination, $timeout]);
    }

    /**
     * @link https://redis.io/commands/lrange
     */
    public function getRange(int $start = 0, int $end = -1): array
    {
        return $this->queryExecutor->execute(['lrange', $this->key, $start, $end]);
    }

    /**
     * @link https://redis.io/commands/lrem
     */
    public function remove(string $value, int $count = 0): int
    {
        return $this->queryExecutor->execute(['lrem', $this->key, $count, $value]);
    }

    /**
     * @link https://redis.io/commands/lset
     */
    public function set(int $index, string $value): void
    {
        $this->queryExecutor->execute(['lset', $this->key, $index, $value], toNull(...));
    }

    /**
     *
     * @link https://redis.io/commands/ltrim
     */
    public function trim(int $start = 0, int $stop = -1): void
    {
        $this->queryExecutor->execute(['ltrim', $this->key, $start, $stop], toNull(...));
    }

    /**
     * @link https://redis.io/commands/sort
     */
    public function sort(?RedisSortOptions $sort = null): array
    {
        return $this->queryExecutor->execute(\array_merge(['SORT', $this->key], ($sort ?? new RedisSortOptions)->toQuery()));
    }
}
