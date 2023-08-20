<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Cache\Cache as CacheInterface;
use Amp\Cache\CacheException;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Redis\Connection\RedisLink;

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
        return new RedisCache(new Redis(new RedisClient(new class implements RedisLink {
            public function execute(string $command, array $parameters): never
            {
                throw new RedisException('Failed, because dummy implementation');
            }
        })));
    }
}
