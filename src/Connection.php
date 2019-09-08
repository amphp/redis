<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Amp\Success;
use League\Uri;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\Socket\connect;

final class Connection
{
    public const STATE_DISCONNECTED = 0;
    public const STATE_CONNECTING = 1;
    public const STATE_CONNECTED = 2;

    /** @var Deferred */
    private $connectPromisor;

    /** @var RespParser */
    private $parser;

    /** @var string */
    private $uri;

    /** @var int */
    private $timeout = 5000;

    /** @var Socket */
    private $socket;

    /** @var array */
    private $handlers;

    /** @var int */
    private $state;

    /**
     * @param string $uri
     */
    public function __construct(string $uri)
    {
        if (\strpos($uri, 'tcp://') !== 0 && \strpos($uri, 'unix://') !== 0) {
            throw new \Error('URI must start with tcp:// or unix://');
        }

        $this->applyUri($uri);

        $this->state = self::STATE_DISCONNECTED;

        $this->handlers = [
            'connect' => [],
            'response' => [],
            'error' => [],
            'close' => [],
        ];

        $this->parser = new RespParser(function ($response) {
            foreach ($this->handlers['response'] as $handler) {
                $handler($response);
            }
        });
    }

    public function addEventHandler($event, callable $callback): void
    {
        $events = (array) $event;

        /** @noinspection SuspiciousLoopInspection */
        foreach ($events as $event) {
            if (!isset($this->handlers[$event])) {
                throw new \Error('Unknown event: ' . $event);
            }

            $this->handlers[$event][] = $callback;
        }
    }

    public function send(array $strings): Promise
    {
        foreach ($strings as $string) {
            if (!\is_scalar($string)) {
                throw new \TypeError('All elements must be of type string or scalar and convertible to a string.');
            }
        }

        return call(function () use ($strings) {
            $this->setIdle(false);

            $payload = '';
            foreach ($strings as $string) {
                $payload .= '$' . \strlen($string) . "\r\n{$string}\r\n";
            }
            $payload = '*' . \count($strings) . "\r\n{$payload}";

            yield $this->connect();
            yield $this->socket->write($payload);
        });
    }

    public function setIdle(bool $idle): void
    {
        if (!$this->socket) {
            return;
        }

        if ($idle) {
            $this->socket->unreference();
        } else {
            $this->socket->reference();
        }
    }

    public function close(): void
    {
        $this->parser->reset();

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }

        foreach ($this->handlers['close'] as $handler) {
            $handler();
        }

        $this->state = self::STATE_DISCONNECTED;
    }

    public function getState(): int
    {
        return $this->state;
    }

    public function __destruct()
    {
        $this->close();
    }

    private function applyUri(string $uri): void
    {
        $parts = Uri\parse($uri);

        $scheme = $parts['scheme'] ?? '';

        if ($scheme === 'tcp') {
            $this->uri = $scheme . '://' . ($parts['host'] ?? '') . ':' . ($parts['port'] ?? 0);
        } else {
            $this->uri = $scheme . '://' . ($parts['path'] ?? '');
        }

        $pairs = Internal\parseUriQuery($parts['query'] ?? '');

        $this->timeout = $pairs['timeout'] ?? $this->timeout;
    }

    private function connect(): Promise
    {
        // If we're in the process of connecting already return that same promise
        if ($this->connectPromisor) {
            return $this->connectPromisor->promise();
        }

        // If a socket exists we know we're already connected
        if ($this->socket) {
            return new Success;
        }

        $this->state = self::STATE_CONNECTING;
        $this->connectPromisor = new Deferred;
        $connectPromise = $this->connectPromisor->promise();
        /** @noinspection PhpUnhandledExceptionInspection */
        $socketPromise = connect($this->uri, (new ConnectContext)->withConnectTimeout($this->timeout));

        $socketPromise->onResolve(function ($error, Socket $socket = null) {
            $connectPromisor = $this->connectPromisor;
            $this->connectPromisor = null;

            if ($error) {
                $this->state = self::STATE_DISCONNECTED;

                $connectException = new ConnectException(
                    'Connection attempt failed',
                    $code = 0,
                    $error
                );

                $this->onError($connectException);
                $connectPromisor->fail($connectException);

                return;
            }

            $this->state = self::STATE_CONNECTED;
            $this->socket = $socket;

            foreach ($this->handlers['connect'] as $handler) {
                $pipelinedCommand = $handler();

                if (!empty($pipelinedCommand)) {
                    $this->socket->write($pipelinedCommand);
                }
            }

            asyncCall(function () use ($socket) {
                while (null !== $chunk = yield $socket->read()) {
                    $this->parser->append($chunk);
                }

                $this->close();
            });

            $connectPromisor->resolve();
        });

        return $connectPromise;
    }

    private function onError(\Throwable $exception): void
    {
        try {
            foreach ($this->handlers['error'] as $handler) {
                $handler($exception);
            }
        } finally {
            $this->close();
        }
    }
}
