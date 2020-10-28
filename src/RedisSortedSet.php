<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\AsyncGenerator;
use Amp\Pipeline;

final class RedisSortedSet
{
    private QueryExecutor $queryExecutor;
    private string $key;

    public function __construct(QueryExecutor $queryExecutor, string $key)
    {
        $this->queryExecutor = $queryExecutor;
        $this->key = $key;
    }

    /**
     * @param array $data
     *
     * @return int
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
     * @return int
     */
    public function getSize(): int
    {
        return $this->queryExecutor->execute(['zcard', $this->key]);
    }

    /**
     * @param int $min
     * @param int $max
     *
     * @return int
     */
    public function count(int $min, int $max): int
    {
        return $this->queryExecutor->execute(['zcount', $this->key, $min, $max]);
    }

    /**
     * @param string $member
     * @param float  $increment
     *
     * @return float
     */
    public function increment(string $member, float $increment = 1): float
    {
        return $this->queryExecutor->execute(['zincrby', $this->key, $increment, $member], toFloat);
    }

    /**
     * @param string[] $keys
     * @param string   $aggregate
     *
     * @return int
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

    /**
     * @param string $min
     * @param string $max
     *
     * @return int
     */
    public function countLexicographically(string $min, string $max): int
    {
        return $this->queryExecutor->execute(['zlexcount', $this->key, $min, $max]);
    }

    /**
     * @param string $member
     *
     * @return int|null
     */
    public function getRank(string $member): ?int
    {
        return $this->queryExecutor->execute(['zrank', $this->key, $member]);
    }

    /**
     * @param string $member
     * @param string ...$members
     *
     * @return int
     */
    public function remove(string $member, string ...$members): int
    {
        return $this->queryExecutor->execute(\array_merge(['zrem', $this->key, $member], $members));
    }

    /**
     * @param string $min
     * @param string $max
     *
     * @return int
     */
    public function removeLexicographicRange(string $min, string $max): int
    {
        return $this->queryExecutor->execute(['zremrangebylex', $this->key, $min, $max]);
    }

    /**
     * @param int $start
     * @param int $stop
     *
     * @return int
     */
    public function removeRankRange(int $start, int $stop): int
    {
        return $this->queryExecutor->execute(['zremrangebyrank', $this->key, $start, $stop]);
    }

    /**
     * @param float $min
     * @param float $max
     *
     * @return int
     */
    public function removeScoreRange(float $min, float $max): int
    {
        return $this->queryExecutor->execute(['zremrangebyscore', $this->key, $min, $max]);
    }

    /**
     * @param string $member
     *
     * @return int|null
     */
    public function getReversedRank(string $member): ?int
    {
        return $this->queryExecutor->execute(['zrevrank', $this->key, $member]);
    }

    /**
     * @param string|null $pattern
     * @param int|null    $count
     *
     * @return Pipeline<array>
     */
    public function scan(?string $pattern = null, ?int $count = null): Pipeline
    {
        return new AsyncGenerator(function () use ($pattern, $count) {
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
        });
    }

    /**
     * @param string $member
     *
     * @return float|null
     */
    public function getScore(string $member): ?float
    {
        return $this->queryExecutor->execute(['zscore', $this->key, $member], toFloat);
    }

    /**
     * @param string[] $keys
     * @param string   $aggregate
     *
     * @return int
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
     * @param SortOptions|null $sort
     *
     * @return array
     *
     * @link https://redis.io/commands/sort
     */
    public function sort(?SortOptions $sort = null): array
    {
        return $this->queryExecutor->execute(\array_merge(['SORT', $this->key], ($sort ?? new SortOptions)->toQuery()));
    }
}
