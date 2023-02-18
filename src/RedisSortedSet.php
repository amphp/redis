<?php declare(strict_types=1);

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

    /**
     * @param array<string, int|float> $data
     *
     * @return int Number of items added.
     */
    public function add(array $data): int
    {
        $payload = ['zadd', $this->key];

        foreach ($data as $member => $score) {
            $payload[] = $score;
            $payload[] = $member;
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @return list<string>
     */
    public function getRange(int $start, int $end, ?RangeOptions $options = null): array
    {
        $options ??= new RangeOptions();
        return $this->queryExecutor->execute(['zrange', $this->key, $start, $end, ...$options->toQuery()]);
    }

    /**
     * @return array<string, float>
     */
    public function getRangeWithScores(int $start, int $end, ?RangeOptions $options = null): array
    {
        $options ??= new RangeOptions();
        return $this->queryExecutor->execute([
            'zrange',
            $this->key,
            $start,
            $end,
            'WITHSCORES',
            ...$options->toQuery(),
        ], static fn ($values) => Internal\toMap($values, Internal\toFloat(...)));
    }

    /**
     * @return list<string>
     */
    public function getRangeByScore(ScoreBoundary $min, ScoreBoundary $max, ?RangeOptions $options = null): array
    {
        $options ??= new RangeOptions();
        return $this->queryExecutor->execute([
            'zrange',
            $this->key,
            $min->toQuery(),
            $max->toQuery(),
            'BYSCORE',
            ...$options->toQuery(),
        ]);
    }

    /**
     * @return array<string, float>
     */
    public function getRangeByScoreWithScores(ScoreBoundary $min, ScoreBoundary $max, ?RangeOptions $options = null): array
    {
        $options ??= new RangeOptions();
        return $this->queryExecutor->execute([
            'zrange',
            $this->key,
            $min->toQuery(),
            $max->toQuery(),
            'BYSCORE',
            'WITHSCORES',
            ...$options->toQuery(),
        ], static fn ($values) => Internal\toMap($values, Internal\toFloat(...)));
    }

    public function getLexicographicRange(LexBoundary $start, LexBoundary $end, ?RangeOptions $options = null): array
    {
        $options ??= new RangeOptions();
        return $this->queryExecutor->execute([
            'zrange',
            $this->key,
            $start->toQuery(),
            $end->toQuery(),
            'BYLEX',
            ...$options->toQuery(),
        ]);
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
        return $this->queryExecutor->execute(['zincrby', $this->key, $increment, $member], Internal\toFloat(...));
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

    public function countLexicographicRange(LexBoundary $min, LexBoundary $max): int
    {
        return $this->queryExecutor->execute(['zlexcount', $this->key, $min->toQuery(), $max->toQuery()]);
    }

    public function getRank(string $member): ?int
    {
        return $this->queryExecutor->execute(['zrank', $this->key, $member]);
    }

    public function remove(string $member, string ...$members): int
    {
        return $this->queryExecutor->execute(['zrem', $this->key, $member, ...\array_values($members)]);
    }

    public function removeLexicographicRange(LexBoundary $min, LexBoundary $max): int
    {
        return $this->queryExecutor->execute(['zremrangebylex', $this->key, $min->toQuery(), $max->toQuery()]);
    }

    public function removeRankRange(int $start, int $stop): int
    {
        return $this->queryExecutor->execute(['zremrangebyrank', $this->key, $start, $stop]);
    }

    public function removeRangeByScore(ScoreBoundary $min, ScoreBoundary $max): int
    {
        return $this->queryExecutor->execute(['zremrangebyscore', $this->key, $min->toQuery(), $max->toQuery()]);
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
        return $this->queryExecutor->execute(['zscore', $this->key, $member], Internal\toFloat(...));
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
    public function sort(?RedisSortOptions $options = null): array
    {
        return $this->queryExecutor->execute(['SORT', $this->key, ...($options ?? new RedisSortOptions)->toQuery()]);
    }
}
