<?php

namespace Amp\Redis;

class SelectTest extends IntegrationTest
{
    public function testConnect(): void
    {
        $this->assertEquals('PONG', $this->redis->echo('PONG'));
    }

    public function testSelect(): void
    {
        $redis1 = $this->createInstance();
        $redis1->select(1);
        $payload = 'bar';
        $redis1->set('foobar', $payload);
        $this->assertSame($payload, $redis1->get('foobar'));

        $this->assertNotSame($payload, $this->redis->get('foobar'));
    }

    public function testSelectOnReconnect(): void
    {
        $redis1 = $this->createInstance();
        $redis1->select(1);
        $redis1->quit();
        $payload = 'bar';
        $redis1->set('foobar', $payload);
        $this->assertSame($payload, $redis1->get('foobar'));

        $this->assertNotSame($payload, $this->redis->get('foobar'));
    }
}
