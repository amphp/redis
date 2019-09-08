<?php

namespace Amp\Redis;

interface QueryExecutorFactory
{
    public function createQueryExecutor(): QueryExecutor;
}
