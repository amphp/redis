<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\AsyncGenerator;
use Amp\Pipeline;

final class RedisMap
{
    private QueryExecutor $queryExecutor;
    private string $key;

    public function __construct(QueryExecutor $queryExecutor, string $key)
    {
        $this->queryExecutor = $queryExecutor;
        $this->key = $key;
    }

    /**
     * @param string $field
     * @param string ...$fields
     *
     * @return int
     *
     * @link https://redis.io/commands/hdel
     */
    public function remove(string $field, string ...$fields): int
    {
        return $this->queryExecutor->execute(\array_merge(['hdel', $this->key, $field], $fields));
    }

    /**
     * @param string $field
     *
     * @return bool
     *
     * @link https://redis.io/commands/hexists
     */
    public function hasKey(string $field): bool
    {
        return $this->queryExecutor->execute(['hexists', $this->key, $field], toBool);
    }

    /**
     * @param string $field
     * @param int    $increment
     *
     * @return int
     *
     * @link https://redis.io/commands/hincrby
     */
    public function increment(string $field, int $increment = 1): int
    {
        return $this->queryExecutor->execute(['hincrby', $this->key, $field, $increment]);
    }

    /**
     * @param string $field
     * @param float  $increment
     *
     * @return float
     *
     * @link https://redis.io/commands/hincrbyfloat
     */
    public function incrementByFloat(string $field, float $increment): float
    {
        return $this->queryExecutor->execute(['hincrbyfloat', $this->key, $field, $increment], toFloat);
    }

    /**
     * @return array
     *
     * @link https://redis.io/commands/hkeys
     */
    public function getKeys(): array
    {
        return $this->queryExecutor->execute(['hkeys', $this->key]);
    }

    /**
     * @return int
     *
     * @link https://redis.io/commands/hlen
     */
    public function getSize(): int
    {
        return $this->queryExecutor->execute(['hlen', $this->key]);
    }

    /**
     * @return array
     *
     * @link https://redis.io/commands/hvals
     * @link https://redis.io/commands/hgetall
     */
    public function getAll(): array
    {
        return $this->queryExecutor->execute(['hgetall', $this->key], toMap);
    }

    /**
     * @param array $data
     *
     * @link https://redis.io/commands/hmset
     */
    public function setValues(array $data): void
    {
        $array = ['hmset', $this->key];

        foreach ($data as $dataKey => $value) {
            $array[] = $dataKey;
            $array[] = $value;
        }

        $this->queryExecutor->execute($array, toNull);
    }

    /**
     * @param string $field
     *
     * @return string
     *
     * @link https://redis.io/commands/hget
     */
    public function getValue(string $field): string
    {
        return $this->queryExecutor->execute(['hget', $this->key, $field]);
    }

    /**
     * @param string $field
     * @param string ...$fields
     *
     * @return array
     *
     * @link https://redis.io/commands/hmget
     */
    public function getValues(string $field, string ...$fields): array
    {
        return $this->queryExecutor->execute(\array_merge(['hmget', $this->key, $field], $fields));
    }

    /**
     * @param string $field
     * @param string $value
     *
     * @return bool
     *
     * @link https://redis.io/commands/hset
     */
    public function setValue(string $field, string $value): bool
    {
        return $this->queryExecutor->execute(['hset', $this->key, $field, $value], toBool);
    }

    /**
     * @param string $field
     * @param string $value
     *
     * @return bool
     *
     * @link https://redis.io/commands/hsetnx
     */
    public function setValueWithoutOverwrite(string $field, string $value): bool
    {
        return $this->queryExecutor->execute(['hsetnx', $this->key, $field, $value], toBool);
    }

    /**
     * @param string $field
     *
     * @return int
     *
     * @link https://redis.io/commands/hstrlen
     */
    public function getLength(string $field): int
    {
        return $this->queryExecutor->execute(['hstrlen', $this->key, $field]);
    }

    /**
     * @param string|null $pattern
     * @param int|null    $count
     *
     * @return Pipeline<array>
     *
     * @link https://redis.io/commands/hscan
     */
    public function scan(?string $pattern = null, ?int $count = null): Pipeline
    {
        return new AsyncGenerator(function () use ($pattern, $count) {
            $cursor = 0;

            do {
                $query = ['HSCAN', $this->key, $cursor];

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
                    yield [$keys[$i], $keys[$i + 1]];
                }
            } while ($cursor !== '0');
        });
    }
}
