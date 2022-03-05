<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

final class RedisHyperLogLog
{
    private QueryExecutor $queryExecutor;
    private string $key;

    public function __construct(QueryExecutor $queryExecutor, string $key)
    {
        $this->queryExecutor = $queryExecutor;
        $this->key = $key;
    }

    /**
     * @link https://redis.io/commands/pfadd
     */
    public function add(string $element, string ...$elements): bool
    {
        return $this->queryExecutor->execute(\array_merge(['pfadd', $this->key, $element], $elements), toBool(...));
    }

    /**
     * @link https://redis.io/commands/pfcount
     */
    public function count(): int
    {
        return $this->queryExecutor->execute(['pfcount', $this->key]);
    }

    /**
     * @link https://redis.io/commands/pfmerge
     */
    public function storeUnion(string $sourceKey, string ...$sourceKeys): void
    {
        $this->queryExecutor->execute(\array_merge(['pfmerge', $this->key, $sourceKey], $sourceKeys), toNull(...));
    }
}
