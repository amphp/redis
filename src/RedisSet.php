<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\Iterator;
use Amp\Producer;
use Amp\Promise;

final class RedisSet
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
     * @param string $member
     * @param string ...$members
     *
     * @return Promise<int>
     */
    public function add(string $member, string ...$members): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['sadd', $this->key, $member], $members));
    }

    /**
     * @return Promise<int>
     */
    public function getSize(): Promise
    {
        return $this->queryExecutor->execute(['scard', $this->key]);
    }

    /**
     * @param string ...$keys
     *
     * @return Promise<array>
     */
    public function diff(string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['sdiff', $this->key], $keys));
    }

    /**
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<int>
     */
    public function storeDiff(string $key, string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['sdiffstore', $this->key, $key], $keys));
    }

    /**
     * @param string ...$keys
     *
     * @return Promise<array>
     */
    public function intersect(string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['sinter', $this->key], $keys));
    }

    /**
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<int>
     */
    public function storeIntersection(string $key, string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['sinterstore', $this->key, $key], $keys));
    }

    /**
     * @param string $member
     *
     * @return Promise<bool>
     */
    public function contains(string $member): Promise
    {
        return $this->queryExecutor->execute(['sismember', $this->key, $member], toBool);
    }

    /**
     * @return Promise<array>
     */
    public function getAll(): Promise
    {
        return $this->queryExecutor->execute(['smembers', $this->key]);
    }

    /**
     * @param string $member
     * @param string $destination
     *
     * @return Promise<bool>
     */
    public function move(string $member, string $destination): Promise
    {
        return $this->queryExecutor->execute(['smove', $this->key, $destination, $member], toBool);
    }

    /**
     * @return Promise<string>
     */
    public function popRandomMember(): Promise
    {
        return $this->queryExecutor->execute(['spop', $this->key]);
    }

    /**
     * @return Promise<string|null>
     */
    public function getRandomMember(): Promise
    {
        return $this->queryExecutor->execute(['srandmember', $this->key]);
    }

    /**
     * @param int $count
     *
     * @return Promise<string[]>
     */
    public function getRandomMembers(int $count): Promise
    {
        return $this->queryExecutor->execute(['srandmember', $this->key, $count]);
    }

    /**
     * @param string $member
     * @param string ...$members
     *
     * @return Promise<int>
     */
    public function remove(string $member, string ...$members): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['srem', $this->key, $member], $members));
    }

    /**
     * @param string ...$keys
     *
     * @return Promise<array>
     */
    public function union(string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['sunion', $this->key], $keys));
    }

    /**
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<int>
     */
    public function storeUnion(string $key, string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['sunionstore', $this->key, $key], $keys));
    }

    /**
     * @param string $pattern
     * @param int    $count
     *
     * @return Iterator
     */
    public function scan(?string $pattern = null, ?int $count = null): Iterator
    {
        return new Producer(function (callable $emit) use ($pattern, $count) {
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

                [$cursor, $keys] = yield $this->queryExecutor->execute($query);

                foreach ($keys as $key) {
                    yield $emit($key);
                }
            } while ($cursor !== '0');
        });
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
