<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\Iterator;
use Amp\Producer;
use Amp\Promise;

final class RedisMap
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
     * @param string $field
     * @param string ...$fields
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/hdel
     */
    public function remove(string $field, string ...$fields): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['hdel', $this->key, $field], $fields));
    }

    /**
     * @param string $field
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/hexists
     */
    public function hasKey(string $field): Promise
    {
        return $this->queryExecutor->execute(['hexists', $this->key, $field], toBool);
    }

    /**
     * @param string $field
     * @param int    $increment
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/hincrby
     */
    public function increment(string $field, int $increment = 1): Promise
    {
        return $this->queryExecutor->execute(['hincrby', $this->key, $field, $increment]);
    }

    /**
     * @param string $field
     * @param float  $increment
     *
     * @return Promise<float>
     *
     * @link https://redis.io/commands/hincrbyfloat
     */
    public function incrementByFloat(string $field, float $increment): Promise
    {
        return $this->queryExecutor->execute(['hincrbyfloat', $this->key, $field, $increment], toFloat);
    }

    /**
     * @return Promise<array>
     *
     * @link https://redis.io/commands/hkeys
     */
    public function getKeys(): Promise
    {
        return $this->queryExecutor->execute(['hkeys', $this->key]);
    }

    /**
     * @return Promise<int>
     *
     * @link https://redis.io/commands/hlen
     */
    public function getSize(): Promise
    {
        return $this->queryExecutor->execute(['hlen', $this->key]);
    }

    /**
     * @return Promise<array>
     *
     * @link https://redis.io/commands/hvals
     * @link https://redis.io/commands/hgetall
     */
    public function getAll(): Promise
    {
        return $this->queryExecutor->execute(['hgetall', $this->key], toMap);
    }

    /**
     * @param array $data
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/hmset
     */
    public function setValues(array $data): Promise
    {
        $array = ['hmset', $this->key];

        foreach ($data as $dataKey => $value) {
            $array[] = $dataKey;
            $array[] = $value;
        }

        return $this->queryExecutor->execute($array, toNull);
    }

    /**
     * @param string $field
     *
     * @return Promise<string|null>
     *
     * @link https://redis.io/commands/hget
     */
    public function getValue(string $field): Promise
    {
        return $this->queryExecutor->execute(['hget', $this->key, $field]);
    }

    /**
     * @param string $field
     * @param string ...$fields
     *
     * @return Promise<array>
     *
     * @link https://redis.io/commands/hmget
     */
    public function getValues(string $field, string ...$fields): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['hmget', $this->key, $field], $fields));
    }

    /**
     * @param string $field
     * @param string $value
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/hset
     */
    public function setValue(string $field, string $value): Promise
    {
        return $this->queryExecutor->execute(['hset', $this->key, $field, $value], toBool);
    }

    /**
     * @param string $field
     * @param string $value
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/hsetnx
     */
    public function setValueWithoutOverwrite(string $field, string $value): Promise
    {
        return $this->queryExecutor->execute(['hsetnx', $this->key, $field, $value], toBool);
    }

    /**
     * @param string $field
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/hstrlen
     */
    public function getLength(string $field): Promise
    {
        return $this->queryExecutor->execute(['hstrlen', $this->key, $field]);
    }

    /**
     * @param string|null $pattern
     * @param int|null    $count
     *
     * @return Iterator<string>
     *
     * @link https://redis.io/commands/hscan
     */
    public function scan(?string $pattern = null, ?int $count = null): Iterator
    {
        return new Producer(function (callable $emit) use ($pattern, $count) {
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

                [$cursor, $keys] = yield $this->queryExecutor->execute($query);

                $count = \count($keys);
                \assert($count % 2 === 0);

                for ($i = 0; $i < $count; $i += 2) {
                    yield $emit([$keys[$i], $keys[$i + 1]]);
                }
            } while ($cursor !== '0');
        });
    }
}
