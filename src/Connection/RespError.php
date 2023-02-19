<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Redis\QueryException;

final class RespError implements RespPayload
{
    public function __construct(
        public readonly string $message,
    ) {
    }

    /**
     * @throws QueryException
     */
    public function unwrap(): never
    {
        throw new QueryException($this->message);
    }
}
