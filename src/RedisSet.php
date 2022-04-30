<?php /** @noinspection DuplicatedCode */

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
        return $this->queryExecutor->execute(\array_merge(['sadd', $this->key, $member], $members));
    }

    public function getSize(): int
    {
        return $this->queryExecutor->execute(['scard', $this->key]);
    }

    public function diff(string ...$keys): array
    {
        return $this->queryExecutor->execute(\array_merge(['sdiff', $this->key], $keys));
    }

    public function storeDiff(string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute(\array_merge(['sdiffstore', $this->key, $key], $keys));
    }

    public function intersect(string ...$keys): array
    {
        return $this->queryExecutor->execute(\array_merge(['sinter', $this->key], $keys));
    }

    public function storeIntersection(string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute(\array_merge(['sinterstore', $this->key, $key], $keys));
    }

    public function contains(string $member): bool
    {
        return $this->queryExecutor->execute(['sismember', $this->key, $member], toBool(...));
    }

    public function getAll(): array
    {
        return $this->queryExecutor->execute(['smembers', $this->key]);
    }

    public function move(string $member, string $destination): bool
    {
        return $this->queryExecutor->execute(['smove', $this->key, $destination, $member], toBool(...));
    }

    public function popRandomMember(): string
    {
        return $this->queryExecutor->execute(['spop', $this->key]);
    }

    public function getRandomMember(): ?string
    {
        return $this->queryExecutor->execute(['srandmember', $this->key]);
    }

    /**
     *
     * @return string[]
     */
    public function getRandomMembers(int $count): array
    {
        return $this->queryExecutor->execute(['srandmember', $this->key, $count]);
    }

    public function remove(string $member, string ...$members): int
    {
        return $this->queryExecutor->execute(\array_merge(['srem', $this->key, $member], $members));
    }

    public function union(string ...$keys): array
    {
        return $this->queryExecutor->execute(\array_merge(['sunion', $this->key], $keys));
    }

    public function storeUnion(string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute(\array_merge(['sunionstore', $this->key, $key], $keys));
    }

    public function scan(?string $pattern = null, ?int $count = null): \Traversable
    {
        $cursor = 0;

        do {
            $query = ['SSCAN', $this->key, $cursor];

            if ($pattern !== null) {
                $query[] = 'MATCH';
                $query[] = $pattern;
            }

            if ($count !== null) {
                $query[] = 'COUNT';
                $query[] = $count;
            }

            [$cursor, $keys] = $this->queryExecutor->execute($query);

            foreach ($keys as $key) {
                yield $key;
            }
        } while ($cursor !== '0');
    }

    /**
     * @param SortOptions $sort
     *
     * @link https://redis.io/commands/sort
     */
    public function sort(?SortOptions $sort = null): array
    {
        return $this->queryExecutor->execute(\array_merge(['SORT', $this->key], ($sort ?? new SortOptions)->toQuery()));
    }
}
