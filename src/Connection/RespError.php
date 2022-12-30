<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Redis\RedisException;

final class RespError implements RespPayload
{
    public function __construct(
        public readonly RedisException $error,
    ) {
    }

    /**
     * @throws RedisException
     */
    public function unwrap(): never
    {
        throw $this->error;
    }
}
