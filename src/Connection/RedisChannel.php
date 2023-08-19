<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Redis\RedisException;

interface RedisChannel
{
    /**
     * @throws RedisException If reading from the socket fails.
     */
    public function receive(): ?RedisResponse;

    /**
     * @throws RedisException If writing to the socket fails.
     */
    public function send(string ...$args): void;

    public function reference(): void;

    public function unreference(): void;

    public function close(): void;

    public function isClosed(): bool;
}
