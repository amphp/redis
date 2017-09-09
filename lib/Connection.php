<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Socket\ClientConnectContext;
use Amp\Socket\Socket;
use Amp\Success;
use Amp\Uri\InvalidUriException;
use Amp\Uri\Uri;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\Socket\connect;

class Connection {
    const STATE_DISCONNECTED = 0;
    const STATE_CONNECTING = 1;
    const STATE_CONNECTED = 2;

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
    public function __construct(string $uri) {
        if (strpos($uri, "tcp://") !== 0 && strpos($uri, "unix://") !== 0) {
            throw new InvalidUriException("URI must start with tcp:// or unix://");
        }

        $this->applyUri($uri);

        $this->state = self::STATE_DISCONNECTED;

        $this->handlers = [
            "connect" => [],
            "response" => [],
            "error" => [],
            "close" => [],
        ];

        $this->parser = new RespParser(function ($response) {
            foreach ($this->handlers["response"] as $handler) {
                $handler($response);
            }
        });
    }

    private function applyUri(string $uri) {
        $uri = new Uri($uri);

        if ($uri->getScheme() === "tcp") {
            $this->uri = $uri->getScheme() . "://" . $uri->getHost() . ":" . $uri->getPort();
        } else {
            $this->uri = $uri->getScheme() . "://" . $uri->getPath();
        }

        $this->timeout = $uri->getQueryParameter("timeout") ?? $this->timeout;
    }

    public function addEventHandler($event, callable $callback) {
        $events = (array) $event;

        foreach ($events as $event) {
            if (!isset($this->handlers[$event])) {
                throw new \Error("Unknown event: " . $event);
            }

            $this->handlers[$event][] = $callback;
        }
    }

    /**
     * @param array $strings
     *
     * @return Promise
     */
    public function send(array $strings): Promise {
        foreach ($strings as $string) {
            if (!is_scalar($string)) {
                throw new \TypeError("All elements must be of type string or scalar and convertible to a string.");
            }
        }

        return call(function () use ($strings) {
            $this->setIdle(false);

            $payload = "";
            foreach ($strings as $string) {
                $payload .= "$" . strlen($string) . "\r\n{$string}\r\n";
            }
            $payload = "*" . count($strings) . "\r\n{$payload}";

            yield $this->connect();
            yield $this->socket->write($payload);
        });
    }

    private function connect(): Promise {
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
        $socketPromise = connect($this->uri, (new ClientConnectContext)->withConnectTimeout($this->timeout));

        $socketPromise->onResolve(function ($error, Socket $socket = null) {
            $connectPromisor = $this->connectPromisor;
            $this->connectPromisor = null;

            if ($error) {
                $this->state = self::STATE_DISCONNECTED;
                $connectException = new ConnectException(
                    "Connection attempt failed",
                    $code = 0,
                    $error
                );
                $this->onError($connectException);
                $connectPromisor->fail($connectException);

                return;
            }

            $this->state = self::STATE_CONNECTED;
            $this->socket = $socket;

            foreach ($this->handlers["connect"] as $handler) {
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

    private function onError(\Throwable $exception) {
        try {
            foreach ($this->handlers["error"] as $handler) {
                $handler($exception);
            }
        } finally {
            $this->close();
        }
    }

    public function setIdle(bool $idle) {
        if (!$this->socket) {
            return;
        }

        if ($idle) {
            $this->socket->unreference();
        } else {
            $this->socket->reference();
        }
    }

    public function close() {
        $this->parser->reset();

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }

        foreach ($this->handlers["close"] as $handler) {
            $handler();
        }

        $this->state = self::STATE_DISCONNECTED;
    }

    public function getState(): int {
        return $this->state;
    }

    public function __destruct() {
        $this->close();
    }
}
