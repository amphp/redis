<?php

namespace Amp\Redis;

use Amp\Cache\Cache as CacheInterface;
use Amp\Cache\CacheException;
use Amp\Failure;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Promise;

class FailingCacheTest extends AsyncTestCase
{
    public function testFailureGet(): \Generator
    {
        $cache = $this->createFailingCache();

        $this->expectException(CacheException::class);

        yield $cache->get('foo');
    }

    public function testFailureSet(): \Generator
    {
        $cache = $this->createFailingCache();

        $this->expectException(CacheException::class);

        yield $cache->set('foo', 'bar');
    }

    public function testFailureDelete(): \Generator
    {
        $cache = $this->createFailingCache();

        $this->expectException(CacheException::class);

        yield $cache->delete('foo');
    }

    private function createFailingCache(): CacheInterface
    {
        return new Cache(new Redis(new class implements QueryExecutor {
            public function execute(array $query, callable $responseTransform = null): Promise
            {
                return new Failure(new RedisException('Failed, because dummy implementation'));
            }
        }));
    }
}
