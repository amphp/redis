<?php

namespace Amp\Redis;

final class RemoteExecutorFactory implements QueryExecutorFactory
{
    private $config;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public function createQueryExecutor(): QueryExecutor
    {
        return new RemoteExecutor($this->config);
    }
}
