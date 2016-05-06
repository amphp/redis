<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;
use Amp\Success;
use DomainException;
use Exception;
use function Amp\cancel;
use function Amp\disable;
use function Amp\enable;
use function Amp\onReadable;
use function Amp\onWritable;
use function Amp\pipe;
use function Amp\Socket\connect;

class Connection {
    const STATE_DISCONNECTED = 0;
    const STATE_CONNECTING = 1;
    const STATE_CONNECTED = 2;

    /** @var Promisor */
    private $connectPromisor;
    /** @var RespParser */
    private $parser;

    /** @var string */
    private $uri;
    /** @var int */
    private $timeout = 1000;
    /** @var int */
    private $persistent = 0;
    /** @var resource */
    private $socket;
    /** @var string */
    private $readWatcher;
    /** @var string */
    private $writeWatcher;

    /** @var string */
    private $outputBuffer;
    /** @var int */
    private $outputBufferLength;

    /** @var array */
    private $handlers;

    /** @var int */
    private $state;

    /**
     * @param string $uri
     */
    public function __construct($uri) {
        if (!is_string($uri)) {
            throw new DomainException(sprintf(
                "URI must be string, %s given",
                gettype($uri)
            ));
        }

        if (strpos($uri, "tcp://") !== 0 && strpos($uri, "unix://") !== 0) {
            throw new DomainException("URI must start with tcp:// or unix://");
        }

        $this->parseUri($uri);

        $this->outputBufferLength = 0;
        $this->outputBuffer = "";
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

    private function parseUri($uri) {
        $parts = explode("?", $uri, 2);

        if (count($parts) === 1) {
            $this->uri = $uri;

            return;
        } else {
            $this->uri = $parts[0];
        }

        $query = $parts[1];
        $params = explode("&", $query);

        foreach ($params as $param) {
            $keyValue = explode("=", $param, 2);
            $key = $keyValue[0];

            if (count($keyValue) === 1) {
                $value = true;
            } else {
                $value = $keyValue[1];
            }

            switch ($key) {
                case "timeout":
                    $this->timeout = (int) $value;
                    break;
                case "persistent":// set persistent connection
                    $this->persistent = (int) $value;
                    break;
            }
        }
    }

    public function addEventHandler($event, callable $callback) {
        $events = (array) $event;

        foreach ($events as $event) {
            if (!isset($this->handlers[$event])) {
                throw new DomainException("Unknown event: " . $event);
            }

            $this->handlers[$event][] = $callback;
        }
    }

    /**
     * @param array $strings
     * @return Promise
     */
    public function send(array $strings) {
        foreach ($strings as $string) {
            if (!is_scalar($string)) {
                throw new \InvalidArgumentException("All elements must be of type string or scalar and convertible to a string.");
            }
        }

        return pipe($this->connect(), function () use ($strings) {
            $payload = "";

            foreach ($strings as $string) {
                $payload .= "$" . strlen($string) . "\r\n{$string}\r\n";
            }

            $payload = "*" . count($strings) . "\r\n{$payload}";

            $this->outputBuffer .= $payload;
            $this->outputBufferLength += strlen($payload);

            if ($this->writeWatcher !== null) {
                enable($this->writeWatcher);
            }
        });
    }

    private function connect() {
        // If we're in the process of connecting already return that same promise
        if ($this->connectPromisor) {
            return $this->connectPromisor->promise();
        }

        // If a read watcher exists we know we're already connected
        if ($this->readWatcher) {
            return new Success($this);
        }

        $this->state = self::STATE_CONNECTING;
        $this->connectPromisor = new Deferred;
        $socketPromise = connect($this->uri, ["timeout" => $this->timeout, "persistent"=>$this->persistent]);

        $onWrite = function ($watcherId) {
            if ($this->outputBufferLength === 0) {
                disable($watcherId);

                return;
            }

            $bytes = @fwrite($this->socket, $this->outputBuffer);

            if ($bytes === 0) {
                $this->state = self::STATE_DISCONNECTED;
                $this->onError(new ConnectException("Connection went away (write)", $code = 1));
            } else {
                $this->outputBuffer = (string) substr($this->outputBuffer, $bytes);
                $this->outputBufferLength -= $bytes;
            }
        };

        $socketPromise->when(function ($error, $socket) use ($onWrite) {
            $connectPromisor = $this->connectPromisor;
            $this->connectPromisor = null;

            if ($error) {
                $this->state = self::STATE_DISCONNECTED;
                $connectPromisor->fail(new ConnectException(
                    "Connection attempt failed", $code = 0, $error
                ));

                return;
            }

            $this->state = self::STATE_CONNECTED;
            $this->socket = $socket;

            foreach ($this->handlers["connect"] as $handler) {
                $pipelinedCommand = $handler();

                if (!empty($pipelinedCommand)) {
                    $this->outputBuffer = $pipelinedCommand . $this->outputBuffer;
                    $this->outputBufferLength += strlen($pipelinedCommand);
                }
            }

            $this->readWatcher = onReadable($this->socket, function () {
                $read = fread($this->socket, 8192);

                if ($read != "") {
                    $this->parser->append($read);
                } elseif (!is_resource($this->socket) || @feof($this->socket)) {
                    $this->state = self::STATE_DISCONNECTED;
                    $this->onError(new ConnectException("Connection went away (read)", $code = 2));
                }
            });

            $this->writeWatcher = onWritable($this->socket, $onWrite, ["enable" => !empty($this->outputBuffer)]);
            $connectPromisor->succeed();
        });

        return $this->connectPromisor->promise();
    }

    private function onError(Exception $exception) {
        foreach ($this->handlers["error"] as $handler) {
            $handler($exception);
        }

        $this->closeSocket();
    }

    private function closeSocket() {
        cancel($this->readWatcher);
        cancel($this->writeWatcher);

        $this->readWatcher = null;
        $this->writeWatcher = null;

        $this->parser->reset();
        $this->outputBuffer = "";
        $this->outputBufferLength = 0;

        if (!$this->persistent && is_resource($this->socket)) {
            @fclose($this->socket);
        }

        foreach ($this->handlers["close"] as $handler) {
            $handler();
        }

        $this->state = self::STATE_DISCONNECTED;
    }

    public function close() {
        $this->closeSocket();
    }

    public function getState() {
        return $this->state;
    }

    public function __destruct() {
        $this->closeSocket();
    }
}
