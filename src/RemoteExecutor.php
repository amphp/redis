<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use League\Uri;
use function Amp\call;

final class RemoteExecutor implements QueryExecutor
{
    /** @var Deferred[] */
    private $deferreds;

    /** @var Connection */
    private $connection;

    /** @var string */
    private $password;

    /** @var int */
    private $database = 0;

    /**
     * @param string $uri
     */
    public function __construct(string $uri)
    {
        $this->applyUri($uri);

        $this->deferreds = [];
        $this->connection = new Connection($uri);

        $this->connection->addEventHandler('response', function ($response) {
            $deferred = \array_shift($this->deferreds);

            if (empty($this->deferreds)) {
                $this->connection->setIdle(true);
            }

            if ($response instanceof \Exception) {
                $deferred->fail($response);
            } else {
                $deferred->resolve($response);
            }
        });

        $this->connection->addEventHandler(['close', 'error'], function ($error = null) {
            if ($error) {
                // Fail any outstanding promises
                while ($this->deferreds) {
                    $deferred = \array_shift($this->deferreds);
                    $deferred->fail($error);
                }
            }
        });

        if (!empty($this->password)) {
            $this->connection->addEventHandler('connect', function () {
                // AUTH must be before any other command, so we unshift it last
                \array_unshift($this->deferreds, new Deferred);

                return "*2\r\n$4\r\rAUTH\r\n$" . \strlen($this->password) . "\r\n{$this->password}\r\n";
            });
        }

        if ($this->database !== 0) {
            $this->connection->addEventHandler('connect', function () {
                // SELECT must be called for every new connection if another database than 0 is used
                \array_unshift($this->deferreds, new Deferred);

                return "*2\r\n$6\r\rSELECT\r\n$" . \strlen($this->database) . "\r\n{$this->database}\r\n";
            });
        }
    }

    /**
     * @return Promise
     */
    public function close(): Promise
    {
        $promise = Promise\all(\array_map(static function (Deferred $deferred) {
            return $deferred->promise();
        }, $this->deferreds));

        $promise->onResolve(function () {
            $this->connection->close();
        });

        return $promise;
    }

    public function getConnectionState(): int
    {
        return $this->connection->getState();
    }

    /**
     * @param string[] $args
     * @param callable $transform
     *
     * @return Promise
     */
    public function execute(array $args, callable $transform = null): Promise
    {
        return call(function () use ($args, $transform) {
            $command = \strtolower($args[0] ?? '');

            $deferred = new Deferred;
            $promise = $deferred->promise();

            $this->deferreds[] = $deferred;

            yield $this->connection->send($args);
            $response = yield $promise;

            if ($command === 'select') {
                $this->database = (int) $args[1];
            } elseif ($command === 'quit') {
                $this->connection->close();
            }

            return $transform ? $transform($response) : $response;
        });
    }

    private function applyUri(string $uri): void
    {
        $pairs = Internal\parseUriQuery(Uri\parse($uri)['query'] ?? '');

        $this->database = (int) ($pairs['database'] ?? 0);
        $this->password = $pairs['password'] ?? null;
    }
}
