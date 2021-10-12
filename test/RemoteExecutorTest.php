<?php

namespace Amp\Redis;

use Amp\Redis\Trainer\ConnectorTrainer;

class RemoteExecutorTest extends IntegrationTest
{
    private $connectorTrainer;

    public function testAutomaticReconnectIfConnectFails(): \Generator
    {
        // Create new instance, because flushAll() is executed first otherwise
        $this->redis = $this->createInstance();

        $this->connectorTrainer->givenConnectFails();

        try {
            yield $this->redis->get('foobar');

            $this->fail('Expected exception');
        } catch (\Throwable $e) {
            $this->assertStringStartsWith('Failed to connect to redis instance', $e->getMessage());
        }

        $this->connectorTrainer->givenConnectIsNotIntercepted();

        // Should not throw the same error again but retry
        $this->assertNull(yield $this->redis->get('foobar'));
    }

    protected function setUp(): void
    {
        $this->connectorTrainer = new ConnectorTrainer;

        parent::setUp();
    }

    protected function createInstance(): Redis
    {
        return new Redis(new RemoteExecutor(Config::fromUri($this->getUri()), $this->connectorTrainer));
    }
}
