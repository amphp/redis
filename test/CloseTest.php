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

    public function testReconnect2(): \Generator
    {
        $redis = $this->createInstance();
        $this->assertEquals('PONG', yield $redis->echo('PONG'));
        $quitPromise = $redis->quit();
        $promise = $redis->echo('PONG');
        yield $quitPromise;
        $this->assertEquals('PONG', yield $promise);
    }
}
