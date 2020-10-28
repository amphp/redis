<?php

namespace Amp\Redis;

interface QueryExecutor
{
    /**
     * @param string[]      $query
     * @param callable|null $responseTransform
     *
     * @return mixed
     *
     * @see toBool()
     * @see toNull()
     * @see toFloat()
     * @see toMap()
     */
    public function execute(array $query, callable $responseTransform = null): mixed;
}
