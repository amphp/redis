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
        string $expectedUri,
        int $expectedDatabase,
        int $expectedTimeout,
        string $expectedPassword
    ): void {
        $config = Config::fromUri($uri);

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

        $this->assertSame(5000, $config->getTimeout());
        $this->assertSame(3000, $config->withTimeout(3000)->getTimeout());
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
            ['tcp://localhost:6379', 'tcp://localhost:6379', 0, 5000, ''],
            ['tcp://localhost:6379?database=1', 'tcp://localhost:6379', 1, 5000, ''],
            ['tcp://localhost:6379?database=1&pass=foobar', 'tcp://localhost:6379', 1, 5000, 'foobar'],
            ['redis://', 'tcp://localhost:6379', 0, 5000, ''],
            ['redis://localhost', 'tcp://localhost:6379', 0, 5000, ''],
            ['redis://localhost:0', 'tcp://localhost:6379', 0, 5000, ''],
            ['redis://localhost:6379', 'tcp://localhost:6379', 0, 5000, ''],
            ['redis://localhost:6379?db=2', 'tcp://localhost:6379', 2, 5000, ''],
            ['redis://:secret@localhost:6379', 'tcp://localhost:6379', 0, 5000, 'secret'],
            ['redis://:secret@localhost:6379/3', 'tcp://localhost:6379', 3, 5000, 'secret'],
            ['redis://:secret@:6379/3?timeout=10000', 'tcp://localhost:6379', 3, 10000, 'secret'],
            ['redis://:secret@foobar/3?timeout=10000', 'tcp://foobar:6379', 3, 10000, 'secret'],
            ['unix:///run/redis.sock', 'unix:///run/redis.sock', 0, 5000, ''],
            ['unix:///run/redis.sock?db=2&password=123', 'unix:///run/redis.sock', 2, 5000, '123'],
        ];
    }
}
