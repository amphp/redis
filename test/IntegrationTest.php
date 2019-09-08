<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;
use function Amp\Promise\wait;

abstract class IntegrationTest extends AsyncTestCase
{
    /** @var Redis */
    protected $redis;

    protected function setUp(): void
    {
        parent::setUp();

        if (!$this->getUri()) {
            $this->markTestSkipped('AMPHP_TEST_REDIS_INSTANCE is not set');
        }

        $this->redis = $this->createInstance();

        wait($this->redis->flushAll());
    }


    final protected function createInstance(): Redis
    {
        return new Redis(new RemoteExecutor($this->getUri()));
    }

    final protected function getUri(): ?string
    {
        return \getenv('AMPHP_TEST_REDIS_INSTANCE') ?: null;
    }
}
