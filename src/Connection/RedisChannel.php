<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Closable;
use Amp\Redis\RedisException;

/**
 * A RedisChannel allows sending and receiving values, but does not contain any reconnect logic or linking responses
 * to requests.
 */
interface RedisChannel extends Closable
{
    /**
     * @throws RedisException If reading from the channel fails.
     */
    public function receive(): ?RedisResponse;

    /**
     * @throws RedisException If writing to the channel fails.
     */
    public function send(string ...$args): void;

    /**
     * @return string A name for debugging purposes, e.g. the connect URI.
     */
    public function getName(): string;

    public function reference(): void;

    public function unreference(): void;
}
