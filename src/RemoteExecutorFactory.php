<?php

namespace Amp\Redis;

use Amp\Redis\Connection\RedisConnector;

final class RemoteExecutorFactory implements QueryExecutorFactory
{
    public function __construct(
        private readonly RedisConfig $config,
        private readonly ?RedisConnector $connector = null,
    ) {
    }

    public function createQueryExecutor(): QueryExecutor
    {
        return new RemoteExecutor($this->config, $this->connector);
    }
}
