<?php

namespace Amp\Redis;

interface QueryExecutor
{
    /**
     * @param array<array-key, int|float|string> $query
     * @param null|\Closure(mixed):mixed $responseResponseTransform
     *
     * @see toBool()
     * @see toNull()
     * @see toFloat()
     * @see toMap()
     */
    public function execute(array $query, ?\Closure $responseResponseTransform = null): mixed;
}
