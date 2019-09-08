<?php

namespace Amp\Redis;

class KeyTest extends IntegrationTest
{
    public function testKeys(): \Generator
    {
        $this->assertEquals([], yield $this->redis->getKeys());
        $this->redis->set('foo', 42);
        $this->assertEquals(['foo'], yield $this->redis->getKeys());
    }

    public function testSetHasDelete(): \Generator
    {
        $this->assertFalse(yield $this->redis->has('foo'));
        yield $this->redis->set('foo', 'bar');
        $this->assertTrue(yield $this->redis->has('foo'));
        yield $this->redis->delete('foo');
        $this->assertFalse(yield $this->redis->has('foo'));
    }
}
