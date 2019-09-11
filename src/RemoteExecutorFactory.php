<?php

namespace Amp\Redis;

use Amp\Socket;

final class RemoteExecutorFactory implements QueryExecutorFactory
{
    private $config;
    private $connector;

    public function __construct(Config $config, ?Socket\Connector $connector = null)
    {
        $this->config = $config;
        $this->connector = $connector ?? Socket\connector();
    }

    public function createQueryExecutor(): QueryExecutor
    {
        return new RemoteExecutor($this->config, $this->connector);
    }
}
