<?php

namespace Amp\Redis;

use Amp\ByteStream\ClosedException;
use Amp\Pipeline;
use Amp\PipelineSource;
use Amp\Promise;
use Amp\Socket\Socket;
use Amp\Success;
use function Amp\await;
use function Amp\defer;

final class RespSocket
{
    private RespParser $parser;

    private ?Socket $socket;

    private Pipeline $pipeline;

    private Promise $backpressure;

    private \Throwable $error;

    public function __construct(Socket $socket)
    {
        $source = new PipelineSource;
        $this->backpressure = new Success;
        $backpressure = &$this->backpressure;

        $this->socket = $socket;
        $this->pipeline = $source->pipe();
        $this->parser = new RespParser(static function ($message) use ($source, &$backpressure): void {
            $backpressure = $source->emit([$message]);
        });

        defer(function () use ($socket, $source): void {
            try {
                while (null !== $chunk = $socket->read()) {
                    $this->parser->append($chunk);
                    await($this->backpressure);
                }

                $source->complete();
            } catch (\Throwable $e) {
                if (isset($this->error)) {
                    $this->error = $e;
                }

                $source->fail($e);
            }

            $this->close();
        });
    }

    public function read(): ?array
    {
        return $this->pipeline->continue();
    }

    public function write(string ...$args): void
    {
        if (isset($this->error)) {
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

        $this->socket->write($payload);
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
