<?php

namespace Amp\Redis;

use Amp\Cache\Cache as CacheInterface;
use Amp\Cache\CacheException;
use Amp\PHPUnit\AsyncTestCase;

class FailingCacheTest extends AsyncTestCase
{
    public function testFailureGet(): void
    {
        $cache = $this->createFailingCache();

        $this->expectException(CacheException::class);

        $cache->get('foo');
    }

    public function testFailureSet(): void
    {
        $cache = $this->createFailingCache();

        $this->expectException(CacheException::class);

        $cache->set('foo', 'bar');
    }

    public function testFailureDelete(): void
    {
        $cache = $this->createFailingCache();

        $this->expectException(CacheException::class);

        $cache->delete('foo');
    }

    private function createFailingCache(): CacheInterface
    {
        return new Cache(new Redis(new class implements QueryExecutor {
            public function execute(array $query, callable $responseTransform = null): mixed
            {
                throw new RedisException('Failed, because dummy implementation');
            }
        }));
    }
}
