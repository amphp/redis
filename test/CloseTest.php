<?php

namespace Amp\Redis;

class CloseTest extends IntegrationTest
{
    public function testReconnect(): \Generator
    {
        $redis = $this->createInstance();
        $this->assertEquals('PONG', yield $redis->echo('PONG'));
        yield $redis->quit();
        $this->assertEquals('PONG', yield $redis->echo('PONG'));
    }
}
