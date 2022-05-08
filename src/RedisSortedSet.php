<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

final class RedisSortedSet
{
    private readonly QueryExecutor $queryExecutor;
    private readonly string $key;

    public function __construct(QueryExecutor $queryExecutor, string $key)
    {
        $this->queryExecutor = $queryExecutor;
        $this->key = $key;
    }

    public function add(array $data): int
    {
        $payload = ['zadd', $this->key];

        foreach ($data as $member => $score) {
            $payload[] = $score;
            $payload[] = $member;
        }

        return $this->queryExecutor->execute($payload);
    }

    public function getSize(): int
    {
        return $this->queryExecutor->execute(['zcard', $this->key]);
    }

    public function count(int $min, int $max): int
    {
        return $this->queryExecutor->execute(['zcount', $this->key, $min, $max]);
    }

    public function increment(string $member, float $increment = 1): float
    {
        return $this->queryExecutor->execute(['zincrby', $this->key, $increment, $member], toFloat(...));
    }

    /**
     * @param string[] $keys
     */
    public function storeIntersection(array $keys, string $aggregate = 'sum'): int
    {
        $payload = ['zinterstore', $this->key, \count($keys)];
        $weights = [];

        if (\count(\array_filter(\array_keys($keys), 'is_string'))) {
            foreach ($keys as $key => $weight) {
                $payload[] = $key;
                $weights[] = $weight;
            }
        } else {
            foreach ($keys as $key) {
                $payload[] = $key;
            }
        }

        if (\count($weights) > 0) {
            $payload[] = 'WEIGHTS';

            foreach ($weights as $weight) {
                $payload[] = $weight;
            }
        }

        if (\strtolower($aggregate) !== 'sum') {
            $payload[] = 'AGGREGATE';
            $payload[] = $aggregate;
        }

        return $this->queryExecutor->execute($payload);
    }

    public function countLexicographically(string $min, string $max): int
    {
        return $this->queryExecutor->execute(['zlexcount', $this->key, $min, $max]);
    }

    public function getRank(string $member): ?int
    {
        return $this->queryExecutor->execute(['zrank', $this->key, $member]);
    }

    public function remove(string $member, string ...$members): int
    {
        return $this->queryExecutor->execute(\array_merge(['zrem', $this->key, $member], $members));
    }

    public function removeLexicographicRange(string $min, string $max): int
    {
        return $this->queryExecutor->execute(['zremrangebylex', $this->key, $min, $max]);
    }

    public function removeRankRange(int $start, int $stop): int
    {
        return $this->queryExecutor->execute(['zremrangebyrank', $this->key, $start, $stop]);
    }

    public function removeScoreRange(float $min, float $max): int
    {
        return $this->queryExecutor->execute(['zremrangebyscore', $this->key, $min, $max]);
    }

    public function getReversedRank(string $member): ?int
    {
        return $this->queryExecutor->execute(['zrevrank', $this->key, $member]);
    }

    /**
     * @return \Traversable<array>
     */
    public function scan(?string $pattern = null, ?int $count = null): \Traversable
    {
        $cursor = 0;

        do {
            $query = ['ZSCAN', $this->key, $cursor];

            if ($pattern !== null) {
                $query[] = 'MATCH';
                $query[] = $pattern;
            }

            if ($count !== null) {
                $query[] = 'COUNT';
                $query[] = $count;
            }

            [$cursor, $keys] = $this->queryExecutor->execute($query);

            $count = \count($keys);
            \assert($count % 2 === 0);

            for ($i = 0; $i < $count; $i += 2) {
                yield [$keys[$i], (float) $keys[$i + 1]];
            }
        } while ($cursor !== '0');
    }

    public function getScore(string $member): ?float
    {
        return $this->queryExecutor->execute(['zscore', $this->key, $member], toFloat(...));
    }

    /**
     * @param string[] $keys
     */
    public function storeUnion(array $keys, string $aggregate = 'sum'): int
    {
        $payload = ['zunionstore', $this->key, \count($keys)];
        $weights = [];

        if (\count(\array_filter(\array_keys($keys), 'is_string'))) {
            foreach ($keys as $key => $weight) {
                $payload[] = $key;
                $weights[] = $weight;
            }
        } else {
            foreach ($keys as $key) {
                $payload[] = $key;
            }
        }

        if (\count($weights) > 0) {
            $payload[] = 'WEIGHTS';

            foreach ($weights as $weight) {
                $payload[] = $weight;
            }
        }

        if (\strtolower($aggregate) !== 'sum') {
            $payload[] = 'AGGREGATE';
            $payload[] = $aggregate;
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @link https://redis.io/commands/sort
     */
    public function sort(?RedisSortOptions $sort = null): array
    {
        return $this->queryExecutor->execute(\array_merge(['SORT', $this->key], ($sort ?? new RedisSortOptions)->toQuery()));
    }
}
