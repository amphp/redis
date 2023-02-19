<?php declare(strict_types=1);

namespace Amp\Redis;

interface QueryExecutor
{
    /**
     * @param non-empty-list<int|float|string> $query
     */
    public function execute(array $query): mixed;
}
