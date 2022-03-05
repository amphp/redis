<?php

namespace Amp\Redis;

use Amp\ByteStream\ClosedException;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;
use Amp\Future;
use Amp\Socket\Socket;
use Revolt\EventLoop;

final class RespSocket
{
    private Socket $socket;

    private ConcurrentIterator $iterator;

    private Future $backpressure;

    public function __construct(Socket $socket)
    {
        $queue = new Queue();
        $this->backpressure = Future::complete();
        $backpressure = &$this->backpressure;

        $this->socket = $socket;
        $this->iterator = $queue->iterate();

        $parser = new RespParser(static function ($message) use ($queue, &$backpressure): void {
            $backpressure = $queue->pushAsync([$message]);
        });

        EventLoop::queue(static function () use (&$backpressure, $socket, $parser, $queue): void {
            try {
                while (null !== $chunk = $socket->read()) {
                    $parser->append($chunk);
                    $backpressure->await();
                }

                $queue->complete();
            } catch (\Throwable $e) {
                $queue->error($e);
            }

            $socket->close();
            $parser->reset();
        });
    }

    public function read(): ?array
    {
        if (!$this->iterator->continue()) {
            return null;
        }

        return $this->iterator->getValue();
    }

    public function write(string ...$args): void
    {
        if ($this->socket->isClosed()) {
            throw new ClosedException('Redis connection already closed');
        }

        $payload = '';
        foreach ($args as $arg) {
            $payload .= '$' . \strlen($arg) . "\r\n{$arg}\r\n";
        }
        $payload = '*' . \count($args) . "\r\n{$payload}";

        $this->socket->write($payload);
    }

    public function reference(): void
    {
        $this->socket->reference();
    }

    public function unreference(): void
    {
        $this->socket->unreference();
    }

    public function close(): void
    {
        $this->socket->close();
    }

    public function isClosed(): bool
    {
        return $this->socket->isClosed();
    }

    public function __destruct()
    {
        $this->close();
    }
}
