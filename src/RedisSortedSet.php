<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\Iterator;
use Amp\Producer;
use Amp\Promise;

final class RedisSortedSet
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
     * @param array<string, int|float> $data
     *
     * @return Promise<int>
     */
    public function add(array $data): Promise
    {
        $payload = ['zadd', $this->key];

        foreach ($data as $member => $score) {
            $payload[] = $score;
            $payload[] = $member;
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @return Promise<list<string>>
     */
    public function getRange(int $start, int $end, ?RangeOptions $options = null): Promise
    {
        $query = ['zrange', $this->key, $start, $end];
        if ($options !== null) {
            $query = \array_merge($query, $options->toQuery());
        }

        return $this->queryExecutor->execute($query);
    }

    /**
     * @return Promise<array<string, float>>
     */
    public function getRangeWithScores(int $start, int $end, ?RangeOptions $options = null): Promise
    {
        $query = ['zrange', $this->key, $start, $end, 'WITHSCORES'];
        if ($options !== null) {
            $query = \array_merge($query, $options->toQuery());
        }

        return $this->queryExecutor->execute($query, toFloatMap);
    }

    /**
     * @return Promise<list<string>>
     */
    public function getRangeByScore(ScoreBoundary $start, ScoreBoundary $end, ?RangeOptions $options = null): Promise
    {
        $query = ['zrange', $this->key, $start->toQuery(), $end->toQuery(), 'BYSCORE'];
        if ($options !== null) {
            $query = \array_merge($query, $options->toQuery());
        }

        return $this->queryExecutor->execute($query);
    }

    /**
     * @return Promise<array<string, float>>
     */
    public function getRangeByScoreWithScores(ScoreBoundary $start, ScoreBoundary $end, ?RangeOptions $options = null): Promise
    {
        $query = ['zrange', $this->key, $start->toQuery(), $end->toQuery(), 'BYSCORE', 'WITHSCORES'];
        if ($options !== null) {
            $query = \array_merge($query, $options->toQuery());
        }

        return $this->queryExecutor->execute($query, toFloatMap);
    }

    /**
     * @return Promise<list<string>>
     */
    public function getLexicographicRange(string $start, string $end, ?RangeOptions $options = null): Promise
    {
        $query = ['zrange', $this->key, $start, $end, 'BYLEX'];
        if ($options !== null) {
            $query = \array_merge($query, $options->toQuery());
        }

        return $this->queryExecutor->execute($query);
    }

    /**
     * @return Promise<int>
     */
    public function getSize(): Promise
    {
        return $this->queryExecutor->execute(['zcard', $this->key]);
    }

    /**
     * @param float $min
     * @param float $max
     *
     * @return Promise<int>
     */
    public function count(float $min, float $max): Promise
    {
        return $this->queryExecutor->execute(['zcount', $this->key, $min, $max]);
    }

    /**
     * @param string $member
     * @param float  $increment
     *
     * @return Promise<float>
     */
    public function increment(string $member, float $increment = 1): Promise
    {
        return $this->queryExecutor->execute(['zincrby', $this->key, $increment, $member], toFloat);
    }

    /**
     * @param string[] $keys
     * @param string   $aggregate
     *
     * @return Promise<int>
     */
    public function storeIntersection(array $keys, string $aggregate = 'sum'): Promise
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
     * @return Promise<int>
     */
    public function countLexicographically(string $min, string $max): Promise
    {
        return $this->queryExecutor->execute(['zlexcount', $this->key, $min, $max]);
    }

    /**
     * @param string $member
     *
     * @return Promise<int|null>
     */
    public function getRank(string $member): Promise
    {
        return $this->queryExecutor->execute(['zrank', $this->key, $member]);
    }

    /**
     * @param string $member
     * @param string ...$members
     *
     * @return Promise<int>
     */
    public function remove(string $member, string ...$members): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['zrem', $this->key, $member], $members));
    }

    /**
     * @param string $min
     * @param string $max
     *
     * @return Promise<int>
     */
    public function removeLexicographicRange(string $min, string $max): Promise
    {
        return $this->queryExecutor->execute(['zremrangebylex', $this->key, $min, $max]);
    }

    /**
     * @param int $start
     * @param int $stop
     *
     * @return Promise<int>
     */
    public function removeRankRange(int $start, int $stop): Promise
    {
        return $this->queryExecutor->execute(['zremrangebyrank', $this->key, $start, $stop]);
    }

    /**
     * @return Promise<int>
     */
    public function removeRangeByScore(ScoreBoundary $min, ScoreBoundary $max): Promise
    {
        return $this->queryExecutor->execute(['zremrangebyscore', $this->key, $min->toQuery(), $max->toQuery()]);
    }

    /**
     * @param float $min
     * @param float $max
     *
     * @return Promise<int>
     *
     * @deprecated Use {@see removeRangeByScore()} instead.
     */
    public function removeScoreRange(float $min, float $max): Promise
    {
        return $this->queryExecutor->execute(['zremrangebyscore', $this->key, $min, $max]);
    }

    /**
     * @param string $member
     *
     * @return Promise<int|null>
     */
    public function getReversedRank(string $member): Promise
    {
        return $this->queryExecutor->execute(['zrevrank', $this->key, $member]);
    }

    /**
     * @param string|null $pattern
     * @param int|null    $count
     *
     * @return Iterator<string>
     */
    public function scan(?string $pattern = null, ?int $count = null): Iterator
    {
        return new Producer(function (callable $emit) use ($pattern, $count) {
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

                [$cursor, $keys] = yield $this->queryExecutor->execute($query);

                $count = \count($keys);
                \assert($count % 2 === 0);

                for ($i = 0; $i < $count; $i += 2) {
                    yield $emit([$keys[$i], (float) $keys[$i + 1]]);
                }
            } while ($cursor !== '0');
        });
    }

    /**
     * @param string $member
     *
     * @return Promise<float|null>
     */
    public function getScore(string $member): Promise
    {
        return $this->queryExecutor->execute(['zscore', $this->key, $member], toFloat);
    }

    /**
     * @param string[] $keys
     * @param string   $aggregate
     *
     * @return Promise<int>
     */
    public function storeUnion(array $keys, string $aggregate = 'sum'): Promise
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
