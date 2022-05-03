<?php

namespace Amp\Redis\Connection;

use Amp\ByteStream\StreamException;
use Amp\Redis\SocketException;

interface RespSocket
{
    /**
     * @return RespPayload|null
     */
    public function read(): ?RespPayload;

    /**
     * @throws SocketException
     * @throws StreamException
     */
    public function write(string ...$args): void;

    public function reference(): void;

    public function unreference(): void;

    public function close(): void;

    public function isClosed(): bool;
}
