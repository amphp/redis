<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Redis\Command\RedisCommands;

abstract class IntegrationTest extends AsyncTestCase
{
    protected RedisCommands $redis;

    protected function setUp(): void
    {
        parent::setUp();

        if (!$this->getUri()) {
            $this->markTestSkipped('AMPHP_TEST_REDIS_INSTANCE is not set');
        }

        $this->redis = $this->createInstance();

        $this->redis->flushAll();
    }

    final protected function createInstance(): RedisCommands
    {
        return new RedisCommands(createRedisClient($this->getUri()));
    }

    final protected function getUri(): ?string
    {
        return \getenv('AMPHP_TEST_REDIS_INSTANCE') ?: null;
    }
}
