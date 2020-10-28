<?php

namespace Amp\Redis;

class KeyTest extends IntegrationTest
{
    public function testKeys(): void
    {
        $this->assertEquals([], $this->redis->getKeys());
        $this->redis->set('foo', 42);
        $this->assertEquals(['foo'], $this->redis->getKeys());
    }

    public function testSetHasDelete(): void
    {
        $this->assertFalse($this->redis->has('foo'));
        $this->redis->set('foo', 'bar');
        $this->assertTrue($this->redis->has('foo'));
        $this->redis->delete('foo');
        $this->assertFalse($this->redis->has('foo'));
    }
}
