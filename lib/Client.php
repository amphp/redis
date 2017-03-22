<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Exception;

class Client extends Redis {
    /** @var Deferred[] */
    private $promisors;
    /** @var Connection */
    private $connection;
    /** @var string */
    private $uri;
    /** @var string */
    private $password;
    /** @var int */
    private $database = 0;

    /**
     * @param string $uri
     * @param array|null $options
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

            if (isset($options["database"])) {
                $this->database = (int) $options["database"];
            }
        }

        $this->applyUri($uri);

        $this->promisors = [];
        $this->connection = new Connection($uri);

        $this->connection->addEventHandler("response", function ($response) {
            $promisor = array_shift($this->promisors);

            if ($response instanceof Exception) {
                $promisor->fail($response);
            } else {
                $promisor->resolve($response);
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

        if ($this->database !== 0) {
            $this->connection->addEventHandler("connect", function () {
                // SELECT must be called for every new connection if another database than 0 is used
                array_unshift($this->promisors, new Deferred);

                return "*2\r\n$6\r\rSELECT\r\n$" . strlen($this->database) . "\r\n{$this->database}\r\n";
            });
        }

        if (!empty($this->password)) {
            $this->connection->addEventHandler("connect", function () {
                // AUTH must be before any other command, so we unshift it last
                array_unshift($this->promisors, new Deferred);

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
                case "database":
                    $this->database = (int) $value;
                    break;

                case "password":
                    $this->password = $value;
                    break;
            }
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
        $promise = Promise\all(array_map(function (Deferred $promisor) {
            return $promisor->promise();
        }, $this->promisors));

        $promise->onResolve(function () {
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
            ? Promise\pipe($promisor->promise(), $transform)
            : $promisor->promise();
    }

    public function getConnectionState() {
        return $this->connection->getState();
    }
}
