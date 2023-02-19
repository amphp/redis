<?php declare(strict_types=1);
/** @noinspection DuplicatedCode */

namespace Amp\Redis;

final class RedisHyperLogLog
{
    public function __construct(
        private readonly QueryExecutor $queryExecutor,
        private readonly string $key,
    ) {
    }

    /**
     * @link https://redis.io/commands/pfadd
     */
    public function add(string $element, string ...$elements): bool
    {
        return (bool) $this->queryExecutor->execute(
            'pfadd',
            $this->key,
            $element,
            ...$elements,
        );
    }

    /**
     * @link https://redis.io/commands/pfcount
     */
    public function count(): int
    {
        return $this->queryExecutor->execute('pfcount', $this->key);
    }

    /**
     * @link https://redis.io/commands/pfmerge
     */
    public function storeUnion(string $sourceKey, string ...$sourceKeys): void
    {
        $this->queryExecutor->execute(
            'pfmerge',
            $this->key,
            $sourceKey,
            ...$sourceKeys,
        );
    }
}
