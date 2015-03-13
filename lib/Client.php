<?php

namespace Amp\Redis;

use Amp\Promise;
use Amp\Promisor;
use Amp\Reactor;
use Amp\Redis\Future as RedisFuture;
use DomainException;
use function Amp\all;
use function Amp\getReactor;

class Client extends Redis {
    /** @var Promisor[] */
    private $promisors;
    /** @var Connection */
    private $connection;

    /**
     * @param string $uri
     * @param string $password
     * @param Reactor $reactor
     */
    public function __construct ($uri, $password, Reactor $reactor = null) {
        if (!is_string($password) && !is_null($password)) {
            throw new DomainException(sprintf(
                "Password must be string or null, %s given",
                gettype($password)
            ));
        }

        $this->connection = new Connection($uri, $reactor);
        $this->connection->watch(function ($response) {
            $promisor = array_shift($this->promisors);

            if ($response instanceof RedisException) {
                $promisor->fail($response);
            } else {
                $promisor->succeed($response);
            }
        });

        $this->connection->when(function ($error) {
            if ($error) {
                // Fail any outstanding promises
                if ($this->promisors) {
                    while ($this->promisors) {
                        $promisor = array_shift($this->promisors);
                        $promisor->fail($error);
                    }
                }
            }
        });

        if (!empty($password)) {
            $this->connection->setConnectCallback(function () use ($password) {
                // AUTH must be before any other command, so we unshift it here
                array_unshift($this->promisors, new RedisFuture);
                return "*2\r\n$4\r\rAUTH\r\n$" . strlen($password) . "\r\n{$password}\r\n";
            });
        }
    }

    /**
     * @return Transaction
     */
    public function transaction () {
        return new Transaction($this);
    }

    /**
     * @return Promise
     */
    public function close () {
        /** @var Promise $promise */
        $promise = all($this->promisors);
        $promise->when(function () {
            $this->connection->close();
        });

        return $promise;
    }

    /**
     * @param string[] $args
     * @param callable $transform
     * @return Promise
     */
    protected function send (array $args, callable $transform = null) {
        $promisor = new RedisFuture($transform);
        $this->promisors[] = $promisor;
        $this->connection->send($args, $promisor);
        return $promisor->promise();
    }
}
