<?php

namespace Amp\Redis;

use Amp\Promise;
use Amp\Uri\InvalidUriException;
use function Amp\call;

class Client extends Redis
{
    /** @var Connection */
    private $connection;

    /**
     * @param string $uri
     * @throws InvalidUriException
     */
    public function __construct(string $uri)
    {
        $this->connection = new Connection(ConnectionConfig::parse($uri));
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
        return $this->connection->close();
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
            $response = yield $this->connection->send($args);

            return $transform ? $transform($response) : $response;
        });
    }

    public function getConnectionState(): int
    {
        return $this->connection->getState();
    }
}
