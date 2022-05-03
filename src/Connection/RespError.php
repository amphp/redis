<?php

namespace Amp\Redis\Connection;

final class RespError implements RespPayload
{
    public function __construct(
        public readonly \Throwable $error,
    ) {
    }

    public function unwrap(): never
    {
        throw $this->error;
    }
}
