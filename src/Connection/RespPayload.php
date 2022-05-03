<?php

namespace Amp\Redis\Connection;

interface RespPayload
{
    /**
     * @return int|string|list<int>|list<string>|null
     */
    public function unwrap(): int|string|array|null;
}
