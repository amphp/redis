<?php

namespace Amp\Redis;

final class RemoteExecutorFactory implements QueryExecutorFactory
{
    private $uri;

    public function __construct(string $uri)
    {
        $this->uri = $uri;
    }

    public function createQueryExecutor(): QueryExecutor
    {
        return new RemoteExecutor($this->uri);
    }
}
