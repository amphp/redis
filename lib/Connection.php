<?php

namespace Amp\Redis;

use Amp\Future;
use Amp\Promise;
use Amp\Promisor;
use Amp\Reactor;
use Amp\Success;
use DomainException;
use Exception;
use Nbsock\Connector;
use function Amp\getReactor;

class Connection implements Promise {
    /** @var Reactor */
    private $reactor;
    /** @var Connector */
    private $connector;
    /** @var RespParser */
    private $parser;
    /** @var Promisor */
    private $connectPromisor;

    private $uri;
    private $socket;
    private $readWatcher;
    private $writeWatcher;
    private $outputBuffer;
    private $outputBufferLength;
    private $connectCallback;

    private $whens;
    private $watchers;

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
        $this->reactor = $reactor ?: getReactor();

        $this->outputBufferLength = 0;
        $this->outputBuffer = "";

        $this->connector = new Connector($reactor);
        $this->parser = new RespParser(function ($response) {
            foreach ($this->watchers as $watcher) {
                $watcher($response);
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

        $this->connectPromisor = new Future;
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
                $this->fail(new ConnectException("Connection went away", $code = 1));
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

            if (isset($this->connectCallback)) {
                $callback = $this->connectCallback;
                $pipelinedCommand = $callback();

                if (!empty($pipelinedCommand)) {
                    $this->outputBuffer = $pipelinedCommand . $this->outputBuffer;
                    $this->outputBufferLength = strlen($this->outputBuffer);
                }
            }

            $this->readWatcher = $this->reactor->onReadable($this->socket, function () {
                $read = fread($this->socket, 8192);

                if ($read != "") {
                    $this->parser->append($read);
                } elseif (!is_resource($this->socket) || @feof($this->socket)) {
                    $this->fail(new ConnectException("Connection went away", $code = 2));
                }
            });

            $this->writeWatcher = $this->reactor->onWritable($this->socket, $onWrite, !empty($this->outputBuffer));
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
    }

    public function setConnectCallback (callable $callback = null) {
        $this->connectCallback = $callback;
    }

    public function send (array $strings, Promisor $promisor) {
        $this->connect()->when(function ($error) use ($strings, $promisor) {
            if ($error) {
                $promisor->fail($error);
            } else {
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
            }
        });
    }

    public function watch (callable $callback) {
        $this->watchers[] = $callback;
    }

    public function when (callable $callback) {
        $this->whens[] = $callback;
    }

    public function close () {
        $this->closeSocket();
    }

    private function fail (Exception $exception) {
        $this->closeSocket();

        foreach ($this->whens as $when) {
            $when($exception);
        }
    }

    public function __destruct () {
        $this->closeSocket();
    }
}
