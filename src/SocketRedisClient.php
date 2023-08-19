<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Redis\Connection\RedisConnection;
use Amp\Redis\Connection\RedisConnector;
use Amp\Redis\Connection\SocketRedisConnection;

final class SocketRedisClient implements RedisClient
{
    private readonly RedisConnection $connection;

    public function __construct(
        RedisConfig $config,
        ?RedisConnector $connector = null,
    ) {
        $this->connection = new SocketRedisConnection($config, $connector);
    }

    public function execute(string $command, int|float|string ...$parameters): mixed
    {
        return $this->connection->execute($command, $parameters)->unwrap();
    }
}
