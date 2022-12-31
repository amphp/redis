<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

/**
 * @psalm-type RespType = int|string|list<mixed>|null
 */
interface RespPayload
{
    /**
     * @return RespType
     */
    public function unwrap(): int|string|array|null;
}
