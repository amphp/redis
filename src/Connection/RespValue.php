<?php

namespace Amp\Redis\Connection;

final class RespValue implements RespPayload
{
    /**
     * @param int|string|list<int>|list<string>|null $value
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
