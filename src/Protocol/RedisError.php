<?php declare(strict_types=1);

namespace Amp\Redis\Protocol;

final class RedisError implements RedisResponse
{
    public function __construct(
        private readonly string $message,
    ) {
    }

    /**
     * @throws QueryException
     */
    public function unwrap(): never
    {
        throw new QueryException($this->message);
    }

    public function getMessage(): string
    {
        return $this->message;
    }
}
