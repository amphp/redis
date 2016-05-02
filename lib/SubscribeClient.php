<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;
use Exception;
use function Amp\all;

class SubscribeClient {
    /** @var Promisor */
    private $authPromisor;
    /** @var Promisor[][] */
    private $promisors;
    /** @var Promisor[][] */
    private $patternPromisors;
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

        $this->promisors = [];
        $this->patternPromisors = [];

        $this->connection = new Connection($this->uri);
        $this->connection->addEventHandler("response", function ($response) {
            if ($this->authPromisor) {
                if ($response instanceof Exception) {
                    $this->authPromisor->fail($response);
                } else {
                    $this->authPromisor->succeed($response);
                }

                $this->authPromisor = null;

                return;
            }

            switch ($response[0]) {
                case "message":
                    foreach ($this->promisors[$response[1]] as $promisor) {
                        $promisor->update($response[2]);
                    }

                    break;

                case "pmessage":
                    foreach ($this->patternPromisors[$response[1]] as $promisor) {
                        $promisor->update([$response[3], $response[2]]);
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

                while ($this->promisors) {
                    $promisorGroup = array_shift($this->promisors);

                    while ($promisorGroup) {
                        $promisor = array_shift($promisorGroup);
                        $promisor->fail($error);
                    }
                }

                while ($this->patternPromisors) {
                    $promisorGroup = array_shift($this->patternPromisors);

                    while ($promisorGroup) {
                        $promisor = array_shift($promisorGroup);
                        $promisor->fail($error);
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
     * @return void
     */
    public function close() {
        $this->connection->close();
    }

    /**
     * @param string $channel
     * @return Subscription
     */
    public function subscribe($channel) {
        $promisor = new Deferred;
        $subscription = new Subscription($promisor->promise(), function() use ($promisor, $channel) {
            $this->unloadPromisor($promisor, $channel);
        });

        $this->promisors[$channel][spl_object_hash($promisor)] = $promisor;

        $promise = $this->connection->send(["subscribe", $channel]);
        $promise->when(function ($error) use ($channel, $promisor) {
            if ($error) {
                $this->unloadPromisor($promisor, $channel);
                $promisor->fail($error);
            }
        });

        return $subscription;
    }

    private function unloadPromisor(Promisor $promisor, $channel) {
        $hash = spl_object_hash($promisor);

        if (isset($this->promisors[$channel][$hash])) {
            unset($this->promisors[$channel][$hash]);

            $promisor->succeed();

            if (empty($this->promisors[$channel])) {
                $this->unsubscribe($channel);
            }
        }
    }

    /**
     * @param string $pattern
     * @return Subscription
     */
    public function pSubscribe($pattern) {
        $promisor = new Deferred;
        $subscription = new Subscription($promisor->promise(), function() use ($promisor, $pattern) {
            $this->unloadPatternPromisor($promisor, $pattern);
        });

        $this->patternPromisors[$pattern][spl_object_hash($promisor)] = $promisor;

        $promise = $this->connection->send(["psubscribe", $pattern]);
        $promise->when(function ($error) use ($pattern, $promisor) {
            if ($error) {
                $this->unloadPatternPromisor($promisor, $pattern);
                $promisor->fail($error);
            }
        });

        return $subscription;
    }

    private function unloadPatternPromisor(Promisor $promisor, $pattern) {
        $hash = spl_object_hash($promisor);

        if (isset($this->patternPromisors[$pattern][$hash])) {
            unset($this->patternPromisors[$pattern][$hash]);

            $promisor->succeed();

            if (empty($this->patternPromisors[$pattern])) {
                $this->pUnsubscribe($pattern);
            }
        }
    }

    /**
     * @param string|string[] $channel
     * @return Promise
     */
    private function unsubscribe($channel = null) {
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
    private function pUnsubscribe($pattern = null) {
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
