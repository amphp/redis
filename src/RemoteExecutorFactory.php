<?php

namespace Amp\Redis;

use Amp\Socket\SocketConnector;

final class RemoteExecutorFactory implements QueryExecutorFactory
{
    public function __construct(
        private readonly Config $config,
        private readonly ?SocketConnector $connector = null
    ) {
    }

    public function createQueryExecutor(): QueryExecutor
    {
        return new RemoteExecutor($this->config, $this->connector);
    }
}
