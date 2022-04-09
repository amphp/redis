<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    /**
     * @dataProvider provideData
     */
    public function test(
        string $uri,
        float $timeout,
        string $expectedUri,
        int $expectedDatabase,
        float $expectedTimeout,
        string $expectedPassword
    ): void {
        $config = Config::fromUri($uri, $timeout);

        self::assertSame($expectedUri, $config->getConnectUri());
        self::assertSame($expectedDatabase, $config->getDatabase());
        self::assertSame($expectedTimeout, $config->getTimeout());
        self::assertSame($expectedPassword, $config->getPassword());

        if ($expectedPassword === '') {
            self::assertFalse($config->hasPassword());
        } else {
            self::assertTrue($config->hasPassword());
        }
    }

    public function testInvalidScheme(): void
    {
        $this->expectException(RedisException::class);

        Config::fromUri('test://');
    }

    public function testInvalidUri(): void
    {
        $this->expectException(RedisException::class);

        Config::fromUri('redis://\0/#\0#');
    }

    public function testWithTimeout(): void
    {
        $config = Config::fromUri('redis://');

        $this->assertSame(5.0, $config->getTimeout());
        $this->assertSame(3.0, $config->withTimeout(3)->getTimeout());
    }

    public function testWithPassword(): void
    {
        $config = Config::fromUri('redis://');

        $this->assertSame('', $config->getPassword());
        $this->assertSame('foobar', $config->withPassword('foobar')->getPassword());
    }

    public function provideData(): array
    {
        return [
            ['tcp://localhost:6379', 5, 'tcp://localhost:6379', 0, 5, ''],
            ['tcp://localhost:6379?database=1', 5, 'tcp://localhost:6379', 1, 5, ''],
            ['tcp://localhost:6379?database=1&pass=foobar', 5, 'tcp://localhost:6379', 1, 5, 'foobar'],
            ['redis://', 5, 'tcp://localhost:6379', 0, 5, ''],
            ['redis://localhost', 5, 'tcp://localhost:6379', 0, 5, ''],
            ['redis://localhost:0', 5, 'tcp://localhost:6379', 0, 5, ''],
            ['redis://localhost:6379', 5, 'tcp://localhost:6379', 0, 5, ''],
            ['redis://localhost:6379?db=2', 5, 'tcp://localhost:6379', 2, 5, ''],
            ['redis://:secret@localhost:6379', 5, 'tcp://localhost:6379', 0, 5, 'secret'],
            ['redis://:secret@localhost:6379/3', 5, 'tcp://localhost:6379', 3, 5, 'secret'],
            ['redis://:secret@:6379/3', 10, 'tcp://localhost:6379', 3, 10, 'secret'],
            ['redis://:secret@foobar/3', 10, 'tcp://foobar:6379', 3, 10, 'secret'],
            ['unix:///run/redis.sock', 5, 'unix:///run/redis.sock', 0, 5, ''],
            ['unix:///run/redis.sock?db=2&password=123', 5, 'unix:///run/redis.sock', 2, 5, '123'],
        ];
    }
}
