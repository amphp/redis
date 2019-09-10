<?php

namespace Amp\Redis;

use Amp\Promise;

interface QueryExecutor
{
    /**
     * @param string[]      $query
     * @param callable|null $responseTransform
     *
     * @return Promise
     *
     * @see toBool()
     * @see toNull()
     * @see toFloat()
     * @see toMap()
     */
    public function execute(array $query, callable $responseTransform = null): Promise;
}
