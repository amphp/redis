<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;

abstract class IntegrationTest extends AsyncTestCase
{
    protected RedisClient $redis;

    protected function setUp(): void
    {
        parent::setUp();

        if (!$this->getUri()) {
            $this->markTestSkipped('AMPHP_TEST_REDIS_INSTANCE is not set');
        }

        $this->redis = $this->createInstance();

        $this->redis->flushAll();
    }

    final protected function createInstance(): RedisClient
    {
        return createRedisClient($this->getUri());
    }

    final protected function getUri(): ?string
    {
        return \getenv('AMPHP_TEST_REDIS_INSTANCE') ?: null;
    }
}
