<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Redis\Connection\RedisConnection;
use Amp\Redis\Connection\RedisConnector;
use Amp\Redis\Connection\RedisSocketConnection;

final class RemoteExecutor implements QueryExecutor
{
    private readonly RedisConnection $connection;

    public function __construct(
        RedisConfig $config,
        ?RedisConnector $connector = null,
    ) {
        $this->connection = new RedisSocketConnection($config, $connector);
    }

    public function execute(array $query): mixed
    {
        return $this->connection->execute($query)->unwrap();
    }
}
