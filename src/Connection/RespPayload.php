<?php

namespace Amp\Redis\Connection;

/**
 * @psalm-type RedisValue = int|string|list<mixed>|null
 */
interface RespPayload
{
    /**
     * @return RedisValue
     */
    public function unwrap(): int|string|array|null;
}
