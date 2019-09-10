<?php

namespace Amp\Redis;

interface QueryExecutorFactory
{
    /**
     * @return QueryExecutor New QueryExecutor instance.
     */
    public function createQueryExecutor(): QueryExecutor;
}
