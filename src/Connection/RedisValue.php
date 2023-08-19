<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

/**
 * @psalm-import-type RedisValueType from RedisPayload
 */
final class RedisValue implements RedisPayload
{
    /**
     * @param RedisValueType $value
     */
    public function __construct(
        private readonly int|string|array|null $value,
    ) {
    }

    public function unwrap(): int|string|array|null
    {
        return $this->value;
    }
}
