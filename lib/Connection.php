<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;
use Amp\Reactor;
use Amp\Success;
use DomainException;
use Exception;
use Nbsock\Connector;
use function Amp\pipe;

class Connection {
    /** @var Reactor */
    private $reactor;
    /** @var Connector */
    private $connector;
    /** @var Promisor */
    private $connectPromisor;
    /** @var RespParser */
    private $parser;

    /** @var string */
    private $uri;
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

    /**
     * @param string $uri
     * @param Reactor $reactor
     */
    public function __construct ($uri, Reactor $reactor) {
        if (!is_string($uri)) {
            throw new DomainException(sprintf(
                "URI must be string, %s given",
                gettype($uri)
            ));
        }

        if (strpos($uri, "tcp://") !== 0 && strpos($uri, "unix://") !== 0) {
            throw new DomainException("URI must start with tcp:// or unix://");
        }

        $this->uri = $uri;
        $this->reactor = $reactor;

        $this->outputBufferLength = 0;
        $this->outputBuffer = "";

        $this->handlers = [
            "connect" => [],
            "response" => [],
            "error" => [],
            "close" => []
        ];

        $this->connector = new Connector($reactor);
        $this->parser = new RespParser(function ($response) {
            foreach ($this->handlers["response"] as $handler) {
                $handler($response);
            }
        });
    }

    private function connect () {
        // If we're in the process of connecting already return that same promise
        if ($this->connectPromisor) {
            return $this->connectPromisor->promise();
        }

        // If a read watcher exists we know we're already connected
        if ($this->readWatcher) {
            return new Success($this);
        }

        $this->connectPromisor = new Deferred;
        $socketPromise = $this->connector->connect($this->uri, $opts = [
            Connector::OP_MS_CONNECT_TIMEOUT => 1000
        ]);

        $onWrite = function (Reactor $reactor, $watcherId) {
            if ($this->outputBufferLength === 0) {
                $reactor->disable($watcherId);
                return;
            }

            $bytes = fwrite($this->socket, $this->outputBuffer);

            if ($bytes === 0) {
                $this->onError(new ConnectException("Connection went away", $code = 1));
            } else {
                $this->outputBuffer = (string) substr($this->outputBuffer, $bytes);
                $this->outputBufferLength -= $bytes;
            }
        };

        $socketPromise->when(function ($error, $socket) use ($onWrite) {
            $connectPromisor = $this->connectPromisor;
            $this->connectPromisor = null;

            if ($error) {
                $connectPromisor->fail(new ConnectException(
                    "Connection attempt failed", $code = 0, $error
                ));

                return;
            }

            $this->socket = $socket;

            foreach ($this->handlers["connect"] as $handler) {
                $pipelinedCommand = $handler();

                if (!empty($pipelinedCommand)) {
                    $this->outputBuffer = $pipelinedCommand . $this->outputBuffer;
                    $this->outputBufferLength += strlen($pipelinedCommand);
                }
            }

            $this->readWatcher = $this->reactor->onReadable($this->socket, function () {
                $read = fread($this->socket, 8192);

                if ($read != "") {
                    $this->parser->append($read);
                } elseif (!is_resource($this->socket) || @feof($this->socket)) {
                    $this->onError(new ConnectException("Connection went away", $code = 2));
                }
            });

            $this->writeWatcher = $this->reactor->onWritable($this->socket, $onWrite, ["enable" => !empty($this->outputBuffer)]);
            $connectPromisor->succeed();
        });

        return $this->connectPromisor->promise();
    }

    private function closeSocket () {
        $this->reactor->cancel($this->readWatcher);
        $this->reactor->cancel($this->writeWatcher);

        $this->readWatcher = null;
        $this->writeWatcher = null;

        $this->parser->reset();
        $this->outputBuffer = "";
        $this->outputBufferLength = 0;

        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }

        foreach ($this->handlers["close"] as $handler) {
            $handler();
        }
    }

    public function addEventHandler ($event, callable $callback) {
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
    public function send (array $strings) {
        return pipe($this->connect(), function () use ($strings) {
            $payload = "";

            foreach ($strings as $string) {
                $payload .= "$" . strlen($string) . "\r\n{$string}\r\n";
            }

            $payload = "*" . count($strings) . "\r\n{$payload}";

            $this->outputBuffer .= $payload;
            $this->outputBufferLength += strlen($payload);

            if ($this->writeWatcher !== null) {
                $this->reactor->enable($this->writeWatcher);
            }
        });
    }

    public function close () {
        $this->closeSocket();
    }

    private function onError (Exception $exception) {
        foreach ($this->handlers["error"] as $handler) {
            $handler($exception);
        }

        $this->closeSocket();
    }

    public function __destruct () {
        $this->closeSocket();
    }
}
