<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;
use DomainException;
use Exception;
use function Amp\all;
use function Amp\pipe;
use function Amp\promises;

class Client extends Redis {
    /** @var Promisor[] */
    private $promisors;
    /** @var Connection */
    private $connection;
    /** @var string */
    private $password;
    /** @var int */
    private $database;

    /**
     * @param string $uri
     * @param array  $options
     */
    public function __construct($uri, array $options = []) {
        $this->applyOptions($options);
        $this->promisors = [];

        $this->connection = new Connection($uri);
        $this->connection->addEventHandler("response", function ($response) {
            $promisor = array_shift($this->promisors);

            if ($response instanceof Exception) {
                $promisor->fail($response);
            } else {
                $promisor->succeed($response);
            }
        });

        $this->connection->addEventHandler(["close", "error"], function ($error = null) {
            if ($error) {
                // Fail any outstanding promises
                while ($this->promisors) {
                    $promisor = array_shift($this->promisors);
                    $promisor->fail($error);
                }
            }
        });

        if ($this->database != 0) {
            $this->connection->addEventHandler("connect", function () {
                // SELECT must be called for every new connection if another database than 0 is used
                array_unshift($this->promisors, new Deferred);

                return "*2\r\n$6\r\rSELECT\r\n$" . strlen($this->database) . "\r\n{$this->database}\r\n";
            });
        }

        if (!empty($this->password)) {
            $this->connection->addEventHandler("connect", function () {
                // AUTH must be before any other command, so we unshift it here
                array_unshift($this->promisors, new Deferred);

                return "*2\r\n$4\r\rAUTH\r\n$" . strlen($this->password) . "\r\n{$this->password}\r\n";
            });
        }
    }

    private function applyOptions(array $options) {
        $this->password = isset($options["password"]) ? $options["password"] : null;

        if (!is_string($this->password) && !is_null($this->password)) {
            throw new DomainException(sprintf(
                "Password must be string or null, %s given",
                gettype($this->password)
            ));
        }

        $this->database = isset($options["database"]) ? $options["database"] : 0;

        if (!is_int($this->database)) {
            throw new DomainException(sprintf(
                "Database must be int, %s given",
                gettype($this->database)
            ));
        }
    }

    /**
     * @return Transaction
     */
    public function transaction() {
        return new Transaction($this);
    }

    /**
     * @return Promise
     */
    public function close() {
        /** @var Promise $promise */
        $promise = all(promises($this->promisors));
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
    protected function send(array $args, callable $transform = null) {
        $promisor = new Deferred;
        $this->connection->send($args);
        $this->promisors[] = $promisor;

        return $transform
            ? pipe($promisor->promise(), $transform)
            : $promisor->promise();
    }

    public function getConnectionState() {
        return $this->connection->getState();
    }
}
