<?php

namespace Amp\Redis\Connection;

use Amp\Redis\RedisException;

interface RespSocket
{
    /**
     * @throws RedisException If reading from the socket fails.
     */
    public function read(): ?RespPayload;

    /**
     * @throws RedisException If writing to the socket fails.
     */
    public function write(string ...$args): void;

    public function reference(): void;

    public function unreference(): void;

    public function close(): void;

    public function isClosed(): bool;
}
