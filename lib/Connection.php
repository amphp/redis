<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Socket\ClientConnectContext;
use Amp\Socket\Socket;
use Amp\Success;
use Exception;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\Socket\connect;

class Connection
{
    const STATE_DISCONNECTED = 0;
    const STATE_CONNECTING = 1;
    const STATE_CONNECTED = 2;

    /** @var Deferred */
    private $connectPromisor;

    /** @var RespParser */
    private $parser;

    /** @var Socket */
    private $socket;

    /** @var array */
    private $handlers;

    /** @var int */
    private $state;

    /** @var ConnectionConfig */
    private $config;

    /** @var Deferred[] */
    private $deferreds;

    /**
     * @param ConnectionConfig $config
     */
    public function __construct(ConnectionConfig $config)
    {
        $this->deferreds = [];
        $this->config = $config;
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

        $this->addEventHandler("response", function ($response) {
            $deferred = \array_shift($this->deferreds);

            if (empty($this->deferreds)) {
                $this->setIdle(true);
            }
            if (!$deferred instanceof Deferred) {
                return;
            }

            if ($response instanceof Exception) {
                $deferred->fail($response);
            } else {
                $deferred->resolve($response);
            }
        });

        $this->addEventHandler(["close", "error"], function ($error = null) {
            if ($error) {
                // Fail any outstanding promises
                while ($this->deferreds) {
                    $deferred = \array_shift($this->deferreds);
                    $deferred->fail($error);
                }
            }
        });

        if ($this->config->hasPassword()) {
            $this->addEventHandler("connect", function () {
                // AUTH must be before any other command, so we unshift it last
                \array_unshift($this->deferreds, new Deferred);
                $password = $this->config->getPassword();

                return "*2\r\n$4\r\rAUTH\r\n$" . \strlen($password) . "\r\n{$password}\r\n";
            });
        }

        if ($this->config->getDatabase() !== 0) {
            $this->addEventHandler("connect", function () {
                // SELECT must be called for every new connection if another database than 0 is used
                \array_unshift($this->deferreds, new Deferred);
                $database = $this->config->getDatabase();

                return "*2\r\n$6\r\rSELECT\r\n$" . \strlen($database) . "\r\n{$database}\r\n";
            });
        }
    }

    public function addEventHandler($event, callable $callback)
    {
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
    public function send(array $strings): Promise
    {
        $deferred = new Deferred;
        $this->deferreds[] = $deferred;

        foreach ($strings as $string) {
            if (!\is_scalar($string)) {
                throw new \TypeError("All elements must be of type string or scalar and convertible to a string.");
            }
        }

        call(function () use ($strings) {
            $this->setIdle(false);

            $payload = "";
            foreach ($strings as $string) {
                $payload .= "$" . \strlen($string) . "\r\n{$string}\r\n";
            }
            $payload = "*" . \count($strings) . "\r\n{$payload}";

            yield $this->connect();
            yield $this->socket->write($payload);
        });

        return $deferred->promise();
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
        $socketPromise = connect(
            $this->config->getUri(),
            (new ClientConnectContext)->withConnectTimeout($this->config->getTimeout())
        );

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

    private function onError(\Throwable $exception)
    {
        try {
            foreach ($this->handlers["error"] as $handler) {
                $handler($exception);
            }
        } finally {
            $this->close();
        }
    }

    public function setIdle(bool $idle)
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

    public function close()
    {
        $promise = Promise\all(\array_map(function (Deferred $deferred) {
            return $deferred->promise();
        }, $this->deferreds));

        $promise->onResolve(function () {
            $this->parser->reset();

            if ($this->socket) {
                $this->socket->close();
                $this->socket = null;
            }

            foreach ($this->handlers["close"] as $handler) {
                $handler();
            }

            $this->state = self::STATE_DISCONNECTED;
        });

        return $promise;
    }

    public function getState(): int
    {
        return $this->state;
    }

    public function __destruct()
    {
        $this->close();
    }
}
