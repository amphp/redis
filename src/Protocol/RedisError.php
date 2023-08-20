<?php declare(strict_types=1);

namespace Amp\Redis\Protocol;

use Amp\Redis\QueryException;

final class RedisError implements RedisResponse
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
