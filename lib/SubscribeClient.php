<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Stream;
use AsyncInterop\Promise;
use Exception;
use Amp\Emitter;
use function Amp\all;

class SubscribeClient {
    /** @var Deferred */
    private $authPromisor;
    /** @var Emitter[][] */
    private $emitters;
    /** @var Emitter[][] */
    private $patternEmitters;
    /** @var Connection */
    private $connection;
    /** @var string */
    private $uri;
    /** @var string */
    private $password;

    /**
     * @param string $uri
     * @param array  $options
     */
    public function __construct($uri, array $options = null) {
        if (is_array($options) || func_num_args() === 2) {
            trigger_error(
                "Using the options array is deprecated and will be removed in the next version. " .
                "Please use the URI to pass options like that: tcp://localhost:6379?database=3&password=abc",
                E_USER_DEPRECATED
            );

            $options = $options ?: [];

            if (isset($options["password"])) {
                $this->password = $options["password"];
            }
        }

        $this->applyUri($uri);

        $this->emitters = [];
        $this->patternEmitters = [];

        $this->connection = new Connection($this->uri);
        $this->connection->addEventHandler("response", function ($response) {
            if ($this->authPromisor) {
                if ($response instanceof Exception) {
                    $this->authPromisor->fail($response);
                } else {
                    $this->authPromisor->resolve($response);
                }

                $this->authPromisor = null;

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
                case "unsubscribe":
                    if ($response[1] === null) {
                        break;
                    }

                    foreach ($this->emitters[$response[1]] as $emitter) {
                        $emitter->resolve();
                    }

                    break;
                case "punsubscribe":
                    if ($response[1] === null) {
                        break;
                    }

                    foreach ($this->patternEmitters[$response[1]] as $emitter) {
                        $emitter->resolve();
                    }

                    break;
                default:
                    break;
            }
        });

        $this->connection->addEventHandler("error", function ($error) {
            if ($error) {
                // Fail any outstanding promises
                if ($this->authPromisor) {
                    $this->authPromisor->fail($error);
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
                $this->authPromisor = new Deferred;

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

    /**
     * @return Promise
     */
    public function close() {
        $streams = [];

        foreach ($this->emitters as $emitterGroup) {
            foreach ($emitterGroup as $emitter) {
                $streams[] = $emitter->stream();
            }
        }

        foreach ($this->patternEmitters as $emitterGroup) {
            foreach ($emitterGroup as $emitter) {
                $streams[] = $emitter->stream();
            }
        }

        /** @var Promise $promise */
        $promise = all($streams);

        $promise->when(function () {
            $this->connection->close();
        });

        $this->unsubscribe();
        $this->pUnsubscribe();

        return $promise;
    }

    /**
     * @param string $channel
     * @return Stream
     */
    public function subscribe($channel) {
        $emitter = new Emitter();

        $promise = $this->connection->send(["subscribe", $channel]);
        $promise->when(function ($error) use ($channel, $emitter) {
            if ($error) {
                $emitter->fail($error);
            } else {
                $this->emitters[$channel][] = $emitter;
                $emitter->stream()->when(function () use ($channel) {
                    array_shift($this->emitters[$channel]);
                });
            }
        });

        return $emitter->stream();
    }

    /**
     * @param string $pattern
     * @return Stream
     */
    public function pSubscribe($pattern) {
        $emitter = new Emitter();

        $promise = $this->connection->send(["psubscribe", $pattern]);
        $promise->when(function ($error) use ($pattern, $emitter) {
            if ($error) {
                $emitter->fail($error);
            } else {
                $this->patternEmitters[$pattern][] = $emitter;
                $emitter->stream()->when(function () use ($pattern) {
                    array_shift($this->patternEmitters[$pattern]);
                });
            }
        });

        return $emitter->stream();
    }

    /**
     * @param string|string[] $channel
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
