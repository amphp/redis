<?php

namespace Amp\Redis;

class SelectTest extends IntegrationTest
{
    public function testConnect(): \Generator
    {
        $this->assertEquals('PONG', yield $this->redis->echo('PONG'));
    }

    public function testSelect(): \Generator
    {
        $redis1 = $this->createInstance();
        yield $redis1->select(1);
        $payload = 'bar';
        yield $redis1->set('foobar', $payload);
        $this->assertSame($payload, yield $redis1->get('foobar'));

        $this->assertNotSame($payload, yield $this->redis->get('foobar'));
    }
}
