<?php declare(strict_types=1);

namespace Amp\Redis;

interface QueryExecutor
{
    /**
     * @param non-empty-list<int|float|string> $query
     * @param null|\Closure(mixed):mixed $responseTransform
     *
     * @see toBool()
     * @see toNull()
     * @see toFloat()
     * @see toMap()
     */
    public function execute(array $query, ?\Closure $responseTransform = null): mixed;
}
