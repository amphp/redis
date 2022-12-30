<?php

namespace Amp\Redis\Connection;

/**
 * @psalm-import-type RedisValue from RespPayload
 */
final class RespValue implements RespPayload
{
    /**
     * @param RedisValue $value
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
