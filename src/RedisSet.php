<?php declare(strict_types=1);
/** @noinspection DuplicatedCode */

namespace Amp\Redis;

final class RedisSet
{
    public function __construct(
        private readonly QueryExecutor $queryExecutor,
        private readonly string $key,
    ) {
    }

    public function add(string $member, string ...$members): int
    {
        return $this->queryExecutor->execute('sadd', $this->key, $member, ...$members);
    }

    public function getSize(): int
    {
        return $this->queryExecutor->execute('scard', $this->key);
    }

    public function diff(string ...$keys): array
    {
        return $this->queryExecutor->execute('sdiff', $this->key, ...$keys);
    }

    public function storeDiff(string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute('sdiffstore', $this->key, $key, ...$keys);
    }

    public function intersect(string ...$keys): array
    {
        return $this->queryExecutor->execute('sinter', $this->key, ...$keys);
    }

    public function storeIntersection(string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute('sinterstore', $this->key, $key, ...$keys);
    }

    public function contains(string $member): bool
    {
        return (bool) $this->queryExecutor->execute('sismember', $this->key, $member);
    }

    public function getAll(): array
    {
        return $this->queryExecutor->execute('smembers', $this->key);
    }

    public function move(string $member, string $destination): bool
    {
        return (bool) $this->queryExecutor->execute('smove', $this->key, $destination, $member);
    }

    public function popRandomMember(): string
    {
        return $this->queryExecutor->execute('spop', $this->key);
    }

    public function getRandomMember(): ?string
    {
        return $this->queryExecutor->execute('srandmember', $this->key);
    }

    /**
     *
     * @return string[]
     */
    public function getRandomMembers(int $count): array
    {
        return $this->queryExecutor->execute('srandmember', $this->key, $count);
    }

    public function remove(string $member, string ...$members): int
    {
        return $this->queryExecutor->execute('srem', $this->key, $member, ...$members);
    }

    public function union(string ...$keys): array
    {
        return $this->queryExecutor->execute('sunion', $this->key, ...$keys);
    }

    public function storeUnion(string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute('sunionstore', $this->key, $key, ...$keys);
    }

    /**
     * @return \Traversable<string>
     */
    public function scan(?string $pattern = null, ?int $count = null): \Traversable
    {
        $cursor = 0;

        do {
            $query = [$this->key, $cursor];

            if ($pattern !== null) {
                $query[] = 'MATCH';
                $query[] = $pattern;
            }

            if ($count !== null) {
                $query[] = 'COUNT';
                $query[] = $count;
            }

            [$cursor, $keys] = $this->queryExecutor->execute('SSCAN', ...$query);

            foreach ($keys as $key) {
                yield $key;
            }
        } while ($cursor !== '0');
    }

    /**
     * @param RedisSortOptions $options
     *
     * @link https://redis.io/commands/sort
     */
    public function sort(?RedisSortOptions $options = null): array
    {
        return $this->queryExecutor->execute('SORT', $this->key, ...($options ?? new RedisSortOptions)->toQuery());
    }
}
