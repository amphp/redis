<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;
use Amp\Reactor;
use DomainException;
use Exception;
use function Amp\all;

class SubscribeClient {
    /** @var Promisor */
    private $authPromisor;
    /** @var Promisor[] */
    private $promisors;
    /** @var Promisor[] */
    private $patternPromisors;
    /** @var Connection */
    private $connection;

    /**
     * @param string $uri
     * @param array $options
     * @param Reactor $reactor
     */
    public function __construct ($uri, $options = [], Reactor $reactor = null) {
        $password = isset($options["password"]) ? $options["password"] : null;

        if (!is_string($password) && !is_null($password)) {
            throw new DomainException(sprintf(
                "Password must be string or null, %s given",
                gettype($password)
            ));
        }

        $this->promisors = [];
        $this->patternPromisors = [];

        $this->connection = new Connection($uri, 0, $reactor);
        $this->connection->watch(function ($response) {
            if ($this->authPromisor) {
                if ($response instanceof Exception) {
                    $this->authPromisor->fail($response);
                } else {
                    $this->authPromisor->succeed($response);
                }

                $this->authPromisor = null;

                return;
            }

            if (!is_array($response)) {
                throw new RedisException(sprintf(
                    "Expecting array, got %s",
                    gettype($response)
                ));
            }

            if (count($response) !== 3 && count($response) !== 4) {
                throw new RedisException(sprintf(
                    "Expecing three or four elements, got %d",
                    count($response)
                ));
            }

            switch ($response[0]) {
                case "subscribe":
                    break;
                case "psubscribe":
                    break;
                case "message":
                    $this->promisors[$response[1]]->update($response[2]);
                    break;
                case "pmessage":
                    // TODO This is only going to work in amp v1.0.0-dev @rdlowrey
                    $this->patternPromisors[$response[1]]->update($response[3], $response[2]);
                    break;
                case "unsubscribe":
                    if ($response[1] === null) {
                        break;
                    }

                    $this->promisors[$response[1]]->succeed();
                    unset($this->promisors[$response[1]]);
                    break;
                case "punsubscribe":
                    if ($response[1] === null) {
                        break;
                    }

                    $this->patternPromisors[$response[1]]->succeed();
                    unset($this->patternPromisors[$response[1]]);
                    break;
            }
        });

        $this->connection->when(function ($error) {
            if ($error) {
                // Fail any outstanding promises
                if ($this->authPromisor) {
                    $this->authPromisor->fail($error);
                }

                while ($this->promisors) {
                    $promisor = array_shift($this->promisors);
                    $promisor->fail($error);
                }

                while ($this->patternPromisors) {
                    $promisor = array_shift($this->patternPromisors);
                    $promisor->fail($error);
                }
            }
        });

        if (!empty($password)) {
            $this->connection->setConnectCallback(function () use ($password) {
                // AUTH must be before any other command, so we unshift it here
                $this->authPromisor = new Deferred;
                return "*2\r\n$4\r\rAUTH\r\n$" . strlen($password) . "\r\n{$password}\r\n";
            });
        }
    }

    /**
     * @return Promise
     */
    public function close () {
        /** @var Promise $promise */
        $promise = all(array_merge($this->promisors, $this->patternPromisors));
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
    public function subscribe ($channel) {
        if (!isset($this->promisors[$channel])) {
            $this->promisors[$channel] = new Deferred;
            $this->connection->send(["subscribe", $channel], $this->promisors[$channel]);
        }

        return $this->promisors[$channel]->promise();
    }

    /**
     * @param string $pattern
     * @return Promise
     */
    public function pSubscribe ($pattern) {
        if (!isset($this->patternPromisors[$pattern])) {
            $this->patternPromisors[$pattern] = new Deferred;
            $this->connection->send(["psubscribe", $pattern], $this->patternPromisors[$pattern]);
        }

        return $this->patternPromisors[$pattern]->promise();
    }

    /**
     * @param string|string[] $channel
     * @return Promise
     */
    public function unsubscribe ($channel = null) {
        if ($channel === null) {
            $promisor = new Deferred;
            $this->connection->send(["unsubscribe"], $promisor);
            return all(array_merge($this->promisors, [$promisor]));
        }

        if (!isset($this->promisors[$channel])) {
            throw new DomainException("Client was not subscribed to {$channel}");
        }

        $this->connection->send(["unsubscribe", $channel], $this->promisors[$channel]);
        return $this->promisors[$channel]->promise();
    }

    /**
     * @param string|string[] $pattern
     * @return Promise
     */
    public function pUnsubscribe ($pattern = null) {
        if ($pattern === null) {
            $promisor = new Deferred;
            $this->connection->send(["punsubscribe"], $promisor);
            return all(array_merge($this->patternPromisors, [$promisor]));
        }

        if (!isset($this->patternPromisors[$pattern])) {
            throw new DomainException("Client was not subscribed to {$pattern}");
        }

        $this->connection->send(["punsubscribe", $pattern], $this->patternPromisors[$pattern]);
        return $this->patternPromisors[$pattern]->promise();
    }
}
