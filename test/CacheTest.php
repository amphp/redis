<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\Cache\Cache as CacheInterface;
use Amp\Cache\Test\CacheTest as BaseCacheTest;

class CacheTest extends BaseCacheTest
{
    protected function createCache(): CacheInterface
    {
        return new Cache(new Redis(new RemoteExecutor(Config::fromUri(\getenv('AMPHP_TEST_REDIS_INSTANCE')))));
    }
}
