<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

/**
 * @psalm-type RedisValueType = int|string|list<mixed>|null
 */
interface RedisPayload
{
    /**
     * @return RedisValueType
     */
    public function unwrap(): int|string|array|null;
}
