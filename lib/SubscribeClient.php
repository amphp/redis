<?php

namespace Amp\Redis;

use function Amp\call;
use Amp\Deferred;
use Amp\Emitter;
use Amp\Promise;
use Exception;

class SubscribeClient {
    /** @var Deferred */
    private $authDeferred;

    /** @var Emitter[][] */
    private $emitters = [];

    /** @var Emitter[][] */
    private $patternEmitters = [];

    /** @var Connection */
    private $connection;

    /** @var string */
    private $uri;

    /** @var string */
    private $password;

    /**
     * @param string $uri
     */
    public function __construct($uri) {
        $this->applyUri($uri);

        $this->connection = new Connection($this->uri);
        $this->connection->addEventHandler("response", function ($response) {
            if ($this->authDeferred) {
                if ($response instanceof Exception) {
                    $this->authDeferred->fail($response);
                } else {
                    $this->authDeferred->resolve($response);
                }

                $this->authDeferred = null;

                return;
            }

            switch ($response[0]) {
                case "message":
                    foreach ($this->emitters[$response[1]] as $emitter) {
                        $emitter->emit($response[2]);
                    }

                    break;

                case "pmessage":
                    foreach ($this->patternEmitters[$response[1]] as $emitter) {
                        $emitter->emit([$response[3], $response[2]]);
                    }

                    break;

                default:
                    break;
            }
        });

        $this->connection->addEventHandler("error", function ($error) {
            if ($error) {
                // Fail any outstanding promises
                if ($this->authDeferred) {
                    $this->authDeferred->fail($error);
                }

                while ($this->emitters) {
                    /** @var Emitter[] $emitterGroup */
                    $emitterGroup = array_shift($this->emitters);

                    while ($emitterGroup) {
                        $emitter = array_shift($emitterGroup);
                        $emitter->fail($error);
                    }
                }

                while ($this->patternEmitters) {
                    /** @var Emitter[] $emitterGroup */
                    $emitterGroup = array_shift($this->patternEmitters);

                    while ($emitterGroup) {
                        $emitter = array_shift($emitterGroup);
                        $emitter->fail($error);
                    }
                }
            }
        });

        if (!empty($this->password)) {
            $this->connection->addEventHandler("connect", function () {
                // AUTH must be before any other command, so we unshift it here
                $this->authDeferred = new Deferred;

                return "*2\r\n$4\r\rAUTH\r\n$" . strlen($this->password) . "\r\n{$this->password}\r\n";
            });
        }
    }

    private function applyUri($uri) {
        $parts = explode("?", $uri, 2);
        $this->uri = $parts[0];

        if (count($parts) === 1) {
            return;
        }

        $query = $parts[1];
        $params = explode("&", $query);

        foreach ($params as $param) {
            $keyValue = explode("=", $param, 2);
            $key = urldecode($keyValue[0]);

            if (count($keyValue) === 1) {
                $value = true;
            } else {
                $value = urldecode($keyValue[1]);
            }

            switch ($key) {
                case "password":
                    $this->password = $value;
                    break;
            }
        }
    }

    public function close() {
        $this->connection->close();
    }

    /**
     * @param string $channel
     *
     * @return Promise
     */
    public function subscribe($channel) {
        return call(function () use ($channel) {
            yield $this->connection->send(["subscribe", $channel]);

            $emitter = new Emitter;
            $this->emitters[$channel][\spl_object_hash($emitter)] = $emitter;

            return $emitter->iterate();
        });
    }

    /**
     * @param string $pattern
     *
     * @return Promise
     */
    public function pSubscribe($pattern) {
        return call(function () use ($pattern) {
            yield $this->connection->send(["psubscribe", $pattern]);

            $emitter = new Emitter;
            $this->patternEmitters[$pattern][\spl_object_hash($emitter)] = $emitter;

            return $emitter->iterate();
        });
    }

    /**
     * @param string|string[] $channel
     *
     * @return Promise
     */
    public function unsubscribe($channel = null) {
        if ($channel === null) {
            // either unsubscribe succeeds and an unsubscribe message
            // will be sent for every channel or promises will fail
            // because of a dead connection.
            return $this->connection->send(["unsubscribe"]);
        }

        return $this->connection->send(["unsubscribe", $channel]);
    }

    /**
     * @param string|string[] $pattern
     *
     * @return Promise
     */
    public function pUnsubscribe($pattern = null) {
        if ($pattern === null) {
            // either unsubscribe succeeds and an unsubscribe message
            // will be sent for every channel or promises will fail
            // because of a dead connection.
            return $this->connection->send(["punsubscribe"]);
        }

        return $this->connection->send(["punsubscribe", $pattern]);
    }

    public function getConnectionState() {
        return $this->connection->getState();
    }
}
