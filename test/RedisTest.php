<?php

namespace Amp\Redis;

use Amp\Iterator;
use function Amp\delay;

class RedisTest extends IntegrationTest
{
    public function test()
    {
        yield $this->redis->flushAll();

        $this->assertTrue(yield $this->redis->set('foo', 'bar'));
        $this->assertSame('bar', yield $this->redis->get('foo'));
        $this->assertSame('bar', yield $this->redis->query('get', 'foo'));
        $this->assertSame('foo', yield $this->redis->getRandomKey());
        $this->assertTrue(yield $this->redis->expireIn('foo', 2));
        $this->assertGreaterThan(1000, yield $this->redis->getTtlInMillis('foo'));
        $this->assertGreaterThanOrEqual(1, yield $this->redis->getTtl('foo'));
        $this->assertTrue(yield $this->redis->persist('foo'));
        $this->assertSame(-1, yield $this->redis->getTtlInMillis('foo'));
        $this->assertSame(-1, yield $this->redis->getTtl('foo'));

        yield delay(3000);

        $this->assertTrue(yield $this->redis->has('foo'));
        $this->assertTrue(yield $this->redis->expireIn('foo', 1));

        yield delay(1500);

        $this->assertFalse(yield $this->redis->has('foo'));

        $this->assertTrue(yield $this->redis->set('foo', 'bar'));
        $this->assertNull(yield $this->redis->rename('foo', 'bar'));
        $this->assertSame('bar', yield $this->redis->get('bar'));

        $this->assertSame(['bar'], yield Iterator\toArray($this->redis->scan()));

        $this->assertTrue(yield $this->redis->set('string', "\xF0"));
        $this->assertSame(4, yield $this->redis->countBits('string'));
        $this->assertSame(2, yield $this->redis->append('string', "\x23"));
        $this->assertSame(7, yield $this->redis->countBits('string'));
        $this->assertSame("\xF0\x23", yield $this->redis->get('string'));
        $this->assertSame("\xF0\x23", yield $this->redis->getRange('string'));
        $this->assertSame("\x23", yield $this->redis->getRange('string', 1));
        $this->assertSame('', yield $this->redis->getRange('string', 2));
        $this->assertSame("\xF0", yield $this->redis->getRange('string', 0, -2));

        $this->assertTrue(yield $this->redis->set('number', 3));
        $this->assertSame(2, yield $this->redis->decrement('number'));
        $this->assertSame(4, yield $this->redis->increment('number', 2));
        $this->assertSame(2, yield $this->redis->decrement('number', 2));
        $this->assertSame('2', yield $this->redis->getAndSet('number', 3));
        $this->assertSame('3', yield $this->redis->get('number'));
        $this->assertSame(4, yield $this->redis->increment('number'));
        $this->assertSame(6, yield $this->redis->increment('number', 2));
        $this->assertSame(1, yield $this->redis->getLength('number'));
    }
}
