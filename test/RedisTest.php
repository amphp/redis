<?php declare(strict_types=1);

namespace Amp\Redis;

use function Amp\delay;

class RedisTest extends IntegrationTest
{
    public function test(): void
    {
        $this->redis->flushAll();

        $this->assertTrue($this->redis->set('foo', 'bar'));
        $this->assertSame('bar', $this->redis->get('foo'));
        $this->assertSame('bar', $this->redis->execute('get', 'foo'));
        $this->assertSame('foo', $this->redis->getRandomKey());
        $this->assertTrue($this->redis->expireIn('foo', 2));
        $this->assertGreaterThan(1000, $this->redis->getTtlInMillis('foo'));
        $this->assertGreaterThanOrEqual(1, $this->redis->getTtl('foo'));
        $this->assertTrue($this->redis->persist('foo'));
        $this->assertSame(-1, $this->redis->getTtlInMillis('foo'));
        $this->assertSame(-1, $this->redis->getTtl('foo'));

        delay(3);

        $this->assertTrue($this->redis->has('foo'));
        $this->assertTrue($this->redis->expireIn('foo', 1));

        delay(1.5);

        $this->assertFalse($this->redis->has('foo'));

        $this->assertTrue($this->redis->set('foo', 'bar'));
        $this->redis->rename('foo', 'bar');
        $this->assertSame('bar', $this->redis->get('bar'));

        $this->assertSame(['bar'], \iterator_to_array($this->redis->scan()));

        $this->assertTrue($this->redis->set('string', "\xF0"));
        $this->assertSame(4, $this->redis->countBits('string'));
        $this->assertSame(2, $this->redis->append('string', "\x23"));
        $this->assertSame(7, $this->redis->countBits('string'));
        $this->assertSame("\xF0\x23", $this->redis->get('string'));
        $this->assertSame("\xF0\x23", $this->redis->getRange('string'));
        $this->assertSame("\x23", $this->redis->getRange('string', 1));
        $this->assertSame('', $this->redis->getRange('string', 2));
        $this->assertSame("\xF0", $this->redis->getRange('string', 0, -2));

        $this->assertSame(3, $this->redis->increment('number', 3));
        $this->assertSame('3', $this->redis->get('number'));
        $this->assertSame(2, $this->redis->decrement('number'));
        $this->assertSame(4, $this->redis->increment('number', 2));
        $this->assertSame(2, $this->redis->decrement('number', 2));
        $this->assertSame('2', $this->redis->getAndSet('number', '3'));
        $this->assertSame('3', $this->redis->get('number'));
        $this->assertSame(4, $this->redis->increment('number'));
        $this->assertSame(6, $this->redis->increment('number', 2));
        $this->assertSame(1, $this->redis->getLength('number'));
    }
}
