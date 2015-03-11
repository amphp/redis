<?php

namespace Amp\Redis;

use Amp\Promise;
use Amp\Promisor;
use Amp\Reactor;
use Amp\Redis\Future as RedisFuture;
use Amp\Success;
use DomainException;
use Nbsock\Connector;
use function Amp\getReactor;

class Client extends Redis {
    /** @var Reactor */
    private $reactor;
    private $connector;
    private $options;
    private $socket;
    private $readWatcher;
    private $writeWatcher;
    private $outputBuffer;
    private $outputBufferLength;
    private $parser;
    /** @var Promisor */
    private $connectPromisor;
    private $promisors = [];
    /** @var Promisor[] */
    private $subscribePromisors = [];
    /** @var Promisor[] */
    private $patternSubscribePromisors = [];
    private $pendingSubscribes = [];
    private $pendingPatternSubscribes = [];
    private $promisesTilSubscribe = [];
    private $subscriptionCount = 0;

    /**
     * @param Reactor $reactor
     * @param array $options
     */
    public function __construct (array $options = [], Reactor $reactor = null) {
        $this->options = [
            "host" => "tcp://127.0.0.1:6379",
            "password" => null
        ];

        if (array_key_exists("host", $options)) {
            $this->setHost($options["host"]);
        }

        if (array_key_exists("password", $options)) {
            $this->setPassword($options["password"]);
        }

        $this->reactor = $reactor ?: getReactor();

        $this->outputBufferLength = 0;
        $this->outputBuffer = "";

        $onResponse = function ($result) {
            if ($this->subscriptionCount === 0) {
                if (empty($this->promisesTilSubscribe)) {
                    $standardMode = true;
                } else {
                    end($this->promisesTilSubscribe);
                    $key = key($this->promisesTilSubscribe);

                    if ($this->promisesTilSubscribe[$key] > 0) {
                        $this->promisesTilSubscribe[$key]--;
                        $standardMode = true;
                    } else {
                        unset($this->promisesTilSubscribe[$key]);
                        $standardMode = false;
                    }
                }
            } else {
                $standardMode = false;
            }

            if ($standardMode) {
                $promisor = array_shift($this->promisors);

                if ($result instanceof RedisException) {
                    $promisor->fail($result);
                } else {
                    $promisor->succeed($result);
                }
            } else {
                if (!is_array($result)) {
                    throw new RedisException(sprintf(
                        "Expecting array, got %s",
                        gettype($result)
                    ));
                }

                if (count($result) !== 3 && count($result) !== 4) {
                    throw new RedisException(sprintf(
                        "Expecing exactly three elements, got %d",
                        count($result)
                    ));
                }

                switch ($result[0]) {
                    case "subscribe":
                        $this->subscribePromisors[$result[1]] = array_shift($this->promisors);
                        break;
                    case "psubscribe":
                        $this->patternSubscribePromisors[$result[1]] = array_shift($this->promisors);
                        break;
                    case "message":
                        $this->subscribePromisors[$result[1]]->update($result[2]);
                        break;
                    case "pmessage":
                        $this->patternSubscribePromisors[$result[1]]->update($result[3]);
                        break;
                    case "unsubscribe":
                        $this->subscribePromisors[$result[1]]->succeed($this);
                        unset($this->subscribePromisors[$result[1]]);
                        unset($this->pendingSubscribes[$result[1]]);
                        $this->subscriptionCount = count($this->pendingSubscribes) + count($this->pendingPatternSubscribes);
                        break;
                    case "punsubscribe":
                        $this->patternSubscribePromisors[$result[1]]->succeed($this);
                        unset($this->patternSubscribePromisors[$result[1]]);
                        unset($this->pendingPatternSubscribes[$result[1]]);
                        $this->subscriptionCount = count($this->pendingSubscribes) + count($this->pendingPatternSubscribes);
                        break;
                }
            }
        };

        $this->connector = new Connector($reactor);
        $this->parser = new RespParser($onResponse);
    }

    private function setHost ($host) {
        if (!is_string($host)) {
            throw new DomainException(sprintf(
                "Host must be string, %s given",
                gettype($host)
            ));
        }

        if (strpos($host, "tcp://") !== 0 && strpos($host, "unix://") !== 0) {
            throw new DomainException("Host must start with tcp:// or unix://");
        }

        $this->options["host"] = $host;
    }

    private function setPassword ($password) {
        if (!is_string($password) || is_null($password)) {
            throw new DomainException(sprintf(
                "Password must be string or null, %s given",
                gettype($password)
            ));
        }

        $this->options["password"] = $password;
    }

    public function transaction () {
        return new Transaction($this);
    }

    /**
     * @param string $channel
     * @return Promise
     */
    public function subscribe ($channel) {
        if (isset($this->subscribePromisors[$channel])) {
            return $this->subscribePromisors[$channel];
        }

        $this->promisesTilSubscribe[] = count($this->promisors);
        $this->pendingSubscribes[$channel] = true;
        $this->subscriptionCount = count($this->pendingSubscribes) + count($this->pendingPatternSubscribes);
        return $this->send(["subscribe", $channel], null);
    }

    public function send (array $strings, callable $transform = null) {
        $promisor = new RedisFuture($transform);
        $this->promisors[] = $promisor;

        $this->connect()->when(function ($error) use ($promisor, $strings) {
            if ($error) {
                $promisor->fail($error);
            } else {
                $payload = "";
                $future = null;

                foreach ($strings as $string) {
                    $payload .= "$" . strlen($string) . "\r\n{$string}\r\n";
                }

                $payload = "*" . count($strings) . "\r\n" . $payload;

                $this->outputBuffer .= $payload;
                $this->outputBufferLength += strlen($payload);

                if ($this->writeWatcher !== null) {
                    $this->reactor->enable($this->writeWatcher);
                }
            }
        });

        return $promisor->promise();
    }

    public function connect () {
        if ($this->connectPromisor) {
            // If we're in the process of connecting already return that same promise
            return $this->connectPromisor->promise();
        }

        if ($this->readWatcher) {
            // If a read watcher exists we know we're already connected
            return new Success($this);
        }

        $this->connectPromisor = new Future;
        $socketPromise = $this->connector->connect($this->options["host"], $opts = [
            Connector::OP_MS_CONNECT_TIMEOUT => 1000
        ]);

        $onWrite = function (Reactor $reactor, $watcherId) {
            if ($this->outputBufferLength === 0) {
                $reactor->disable($watcherId);
                return;
            }

            $bytes = fwrite($this->socket, $this->outputBuffer);

            if ($bytes === 0) {
                $this->close(true);
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

            if (isset($this->options["password"])) {
                // AUTH must be before any other command, so we unshift it here
                $pass = $this->options["password"];
                array_unshift($this->promisors, new Future);
                $this->outputBuffer = "*2\r\n$4\r\rAUTH\r\n$" . strlen($pass) . "\r\n{$pass}\r\n" . $this->outputBuffer;
                $this->outputBufferLength = strlen($this->outputBuffer);
            }

            $this->readWatcher = $this->reactor->onReadable($this->socket, function () {
                $read = fread($this->socket, 8192);

                if ($read != "") {
                    $this->parser->append($read);
                } elseif (!is_resource($this->socket) || @feof($this->socket)) {
                    $this->close(true);
                }
            });

            $this->writeWatcher = $this->reactor->onWritable($this->socket, $onWrite, !empty($this->outputBuffer));

            $connectPromisor->succeed($this);
        });

        return $this->connectPromisor->promise();
    }

    public function close ($immediately = false) {
        if ($immediately) {
            $this->closeSocket();
            $this->parser->reset();
            $this->outputBuffer = "";
            $this->outputBufferLength = 0;

            // Fail any outstanding promises
            if ($this->promisors) {
                $error = new ConnectException("Connection went away");

                while ($this->promisors) {
                    $promisor = array_shift($this->promisors);
                    $promisor->fail($error);
                }
            }

            return new Success($this);
        }

        if (empty($this->promisors)) {
            return new Success($this);
        }

        $remaining = sizeof($this->promisors);
        $promisor = new Future;

        foreach ($this->promisors as $resolvable) {
            if (!$resolvable instanceof Promise) {
                $resolvable = new Success($resolvable);
            }

            $resolvable->when(function () use (&$remaining, $promisor) {
                if (--$remaining === 0) {
                    $this->closeSocket();
                    $this->parser->reset();
                    $promisor->succeed($this);
                }
            });
        }

        return $promisor;
    }

    private function closeSocket () {
        $this->reactor->cancel($this->readWatcher);
        $this->reactor->cancel($this->writeWatcher);

        $this->readWatcher = null;
        $this->writeWatcher = null;

        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }
    }

    /**
     * @param string $pattern
     * @return Promise
     */
    public function pSubscribe ($pattern) {
        if (isset($this->patternSubscribePromisors[$pattern])) {
            return $this->patternSubscribePromisors[$pattern];
        }

        $this->promisesTilSubscribe[] = count($this->promisors);
        $this->pendingPatternSubscribes[$pattern] = true;
        $this->subscriptionCount = count($this->pendingSubscribes) + count($this->pendingPatternSubscribes);
        return $this->send(["psubscribe", $pattern], null);
    }

    /**
     * @param string|string[] $channels
     * @return Promise
     */
    public function unsubscribe ($channels) {
        return $this->send(array_merge(["unsubscribe"], (array) $channels), null);
    }

    /**
     * @param string|string[] $patterns
     * @return Promise
     */
    public function pUnsubscribe ($patterns) {
        return $this->send(array_merge(["punsubscribe"], (array) $patterns), null);
    }

    public function __destruct () {
        $this->close(true);
    }
}
