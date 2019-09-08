<?php

namespace Amp\Redis;

use Amp\Delayed;

class BasicTest extends IntegrationTest
{
    public function testConnect(): \Generator
    {
        $this->assertEquals('PONG', yield $this->createInstance()->echo('PONG'));
    }

    public function testLongPayload(): \Generator
    {
        $redis = $this->createInstance();
        $payload = \str_repeat('a', 6000000);
        yield $redis->set('foobar', $payload);
        $this->assertEquals($payload, yield $redis->get('foobar'));
    }

    public function testAcceptsOnlyScalars(): \Generator
    {
        $this->expectException(\TypeError::class);

        $redis = $this->createInstance();
        /** @noinspection PhpParamsInspection */
        yield $redis->set('foobar', ['abc']);
    }

    public function testMultiCommand(): \Generator
    {
        $redis = $this->createInstance();
        $redis->echo('1');
        $this->assertEquals('2', (yield $redis->echo('2')));
    }

    /**
     * @medium
     */
    public function testTimeout(): \Generator
    {
        $redis = $this->createInstance();
        yield $redis->echo('1');
        yield new Delayed(8000);
        $this->assertEquals('2', (yield $redis->echo('2')));
    }
}
