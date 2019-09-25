<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\Promise;

final class RedisHyperLogLog
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
     * @param string $element
     * @param string ...$elements
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/pfadd
     */
    public function add(string $element, string ...$elements): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['pfadd', $this->key, $element], $elements), toBool);
    }

    /**
     * @return Promise<int>
     *
     * @link https://redis.io/commands/pfcount
     */
    public function count(): Promise
    {
        return $this->queryExecutor->execute(['pfcount', $this->key]);
    }

    /**
     * @param string $sourceKey
     * @param string ...$sourceKeys
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/pfmerge
     */
    public function storeUnion(string $sourceKey, string ...$sourceKeys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['pfmerge', $this->key, $sourceKey], $sourceKeys), toNull);
    }
}
