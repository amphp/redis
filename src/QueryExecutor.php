<?php

namespace Amp\Redis;

use Amp\Promise;

interface QueryExecutor
{
    public function execute(array $query, callable $responseTransform = null): Promise;
}
