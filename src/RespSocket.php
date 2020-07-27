<?php

namespace Amp\Redis;

use Amp\ByteStream\ClosedException;
use Amp\Emitter;
use Amp\Iterator;
use Amp\Promise;
use Amp\Socket\Socket;
use Amp\Success;
use function Amp\asyncCall;
use function Amp\call;

final class RespSocket
{
    /** @var RespParser */
    private $parser;

    /** @var Socket */
    private $socket;

    /** @var Iterator */
    private $iterator;

    /** @var Promise */
    private $backpressure;

    /** @var \Throwable */
    private $error;

    public function __construct(Socket $socket)
    {
        $emitter = new Emitter;
        $backpressure = &$this->backpressure;
        $backpressure = new Success;

        $this->socket = $socket;
        $this->iterator = $emitter->iterate();
        $this->parser = new RespParser(static function ($message) use ($emitter, &$backpressure) {
            $backpressure = $emitter->emit($message);
        });

        asyncCall(function () use ($socket, $emitter) {
            try {
                while (null !== $chunk = yield $socket->read()) {
                    $this->parser->append($chunk);
                    yield $this->backpressure;
                }

                $emitter->fail(new ClosedException('Socket closed'));
            } catch (\Throwable $e) {
                if ($this->error === null) {
                    $this->error = $e;
                }

                $emitter->fail($e);
            }

            $this->close();
        });
    }

    public function read(): Promise
    {
        return call(function () {
            if (yield $this->iterator->advance()) {
                return [$this->iterator->getCurrent()];
            }

            return null;
        });
    }

    public function write(string ...$args): Promise
    {
        return call(function () use ($args) {
            if ($this->error) {
                throw $this->error;
            }

            if ($this->socket === null) {
                throw new ClosedException('Redis connection already closed');
            }

            $payload = '';
            foreach ($args as $arg) {
                $payload .= '$' . \strlen($arg) . "\r\n{$arg}\r\n";
            }
            $payload = '*' . \count($args) . "\r\n{$payload}";

            yield $this->socket->write($payload);
        });
    }

    public function reference(): void
    {
        if ($this->socket) {
            $this->socket->reference();
        }
    }

    public function unreference(): void
    {
        if ($this->socket) {
            $this->socket->unreference();
        }
    }

    public function close(): void
    {
        if ($this->parser) {
            $this->parser->reset();
        }

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }
    }

    public function isClosed(): bool
    {
        return $this->socket === null;
    }

    public function __destruct()
    {
        $this->close();
    }
}
