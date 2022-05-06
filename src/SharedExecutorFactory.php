<?php

namespace Amp\Redis;

final class SharedExecutorFactory implements QueryExecutorFactory
{
    private ?QueryExecutor $executor = null;

    public function __construct(
        private readonly QueryExecutorFactory $factory,
    ) {
    }

    public function createQueryExecutor(): QueryExecutor
    {
        return $this->executor ??= $this->factory->createQueryExecutor();
    }
}
