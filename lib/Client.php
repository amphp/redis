<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Uri\InvalidUriException;
use Exception;
use function Amp\call;

class Client extends Redis
{
    /** @var Deferred[] */
    private $deferreds;

    /** @var Connection */
    private $connection;

    /**
     * @param string $uri
     * @throws InvalidUriException
     */
    public function __construct(string $uri)
    {
        $this->deferreds = [];
        $this->connection = new Connection($uri);

        $this->connection->addEventHandler("response", function ($response) {
            $deferred = \array_shift($this->deferreds);

            if (empty($this->deferreds)) {
                $this->connection->setIdle(true);
            }

            if ($response instanceof Exception) {
                $deferred->fail($response);
            } else {
                $deferred->resolve($response);
            }
        });

        $this->connection->addEventHandler(["close", "error"], function ($error = null) {
            if ($error) {
                // Fail any outstanding promises
                while ($this->deferreds) {
                    $deferred = \array_shift($this->deferreds);
                    $deferred->fail($error);
                }
            }
        });

        if (!empty($this->connection->getPassword())) {
            $this->connection->addEventHandler("connect", function () {
                // AUTH must be before any other command, so we unshift it last
                \array_unshift($this->deferreds, new Deferred);
                $password = $this->connection->getPassword();

                return "*2\r\n$4\r\rAUTH\r\n$" . \strlen($password) . "\r\n{$password}\r\n";
            });
        }

        if ($this->connection->getDatabase() !== 0) {
            $this->connection->addEventHandler("connect", function () {
                // SELECT must be called for every new connection if another database than 0 is used
                \array_unshift($this->deferreds, new Deferred);
                $database = $this->connection->getDatabase();

                return "*2\r\n$6\r\rSELECT\r\n$" . \strlen($database) . "\r\n{$database}\r\n";
            });
        }
    }

    /**
     * @return Transaction
     */
    public function transaction(): Transaction
    {
        return new Transaction($this);
    }

    /**
     * @return Promise
     */
    public function close(): Promise
    {
        $promise = Promise\all(\array_map(function (Deferred $deferred) {
            return $deferred->promise();
        }, $this->deferreds));

        $promise->onResolve(function () {
            $this->connection->close();
        });

        return $promise;
    }

    /**
     * @param string[] $args
     * @param callable $transform
     *
     * @return Promise
     */
    protected function send(array $args, callable $transform = null): Promise
    {
        return call(function () use ($args, $transform) {
            $deferred = new Deferred;
            $promise = $deferred->promise();

            $this->deferreds[] = $deferred;

            yield $this->connection->send($args);
            $response = yield $promise;

            return $transform ? $transform($response) : $response;
        });
    }

    public function getConnectionState(): int
    {
        return $this->connection->getState();
    }
}
