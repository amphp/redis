<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;
use DomainException;
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

    /**
     * @param string $uri
     * @param array  $options
     */
    public function __construct($uri, $options = []) {
        $password = isset($options["password"]) ? $options["password"] : null;

        if (!is_string($password) && !is_null($password)) {
            throw new DomainException(sprintf(
                "Password must be string or null, %s given",
                gettype($password)
            ));
        }

        $this->promisors = [];
        $this->patternPromisors = [];

        $this->connection = new Connection($uri);
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
                case "unsubscribe":
                    if ($response[1] === null) {
                        break;
                    }

                    foreach ($this->promisors[$response[1]] as $promisor) {
                        $promisor->succeed();
                    }

                    break;
                case "punsubscribe":
                    if ($response[1] === null) {
                        break;
                    }

                    foreach ($this->patternPromisors[$response[1]] as $promisor) {
                        $promisor->succeed();
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

        if (!empty($password)) {
            $this->connection->addEventHandler("connect", function () use ($password) {
                // AUTH must be before any other command, so we unshift it here
                $this->authPromisor = new Deferred;

                return "*2\r\n$4\r\rAUTH\r\n$" . strlen($password) . "\r\n{$password}\r\n";
            });
        }
    }

    /**
     * @return Promise
     */
    public function close() {
        $promises = [];

        foreach ($this->promisors as $promisorGroup) {
            foreach ($promisorGroup as $promisor) {
                $promises[] = $promisor->promise();
            }
        }

        foreach ($this->patternPromisors as $promisorGroup) {
            foreach ($promisorGroup as $promisor) {
                $promises[] = $promisor->promise();
            }
        }

        /** @var Promise $promise */
        $promise = all($promises);

        $promise->when(function () {
            $this->connection->close();
        });

        $this->unsubscribe();
        $this->pUnsubscribe();

        return $promise;
    }

    /**
     * @param string $channel
     * @return Promise
     */
    public function subscribe($channel) {
        $promisor = new Deferred;

        $promise = $this->connection->send(["subscribe", $channel]);
        $promise->when(function ($error) use ($channel, $promisor) {
            if ($error) {
                $promisor->fail($error);
            } else {
                $this->promisors[$channel][] = $promisor;
                $promisor->promise()->when(function () use ($channel) {
                    array_shift($this->promisors[$channel]);
                });
            }
        });

        return $promisor->promise();
    }

    /**
     * @param string $pattern
     * @return Promise
     */
    public function pSubscribe($pattern) {
        $promisor = new Deferred;

        $promise = $this->connection->send(["psubscribe", $pattern]);
        $promise->when(function ($error) use ($pattern, $promisor) {
            if ($error) {
                $promisor->fail($error);
            } else {
                $this->patternPromisors[$pattern][] = $promisor;
                $promisor->promise()->when(function () use ($pattern) {
                    array_shift($this->patternPromisors[$pattern]);
                });
            }
        });

        return $promisor->promise();
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
