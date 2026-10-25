<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\ByteStream\StreamException;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Socket\InternetAddress;
use Amp\Socket\Socket;

class SocketRedisConnectionTest extends AsyncTestCase
{
    public function testReadFailureErrorsPendingReceive(): void
    {
        $this->setTimeout(1);

        $socket = $this->createMock(Socket::class);
        $socket->method('getRemoteAddress')
            ->willReturn(InternetAddress::fromString('127.0.0.1:6379'));
        $socket->method('read')
            ->willThrowException(new StreamException('Connection reset by peer'));
        $socket->expects(self::atLeastOnce())
            ->method('close');

        $connection = new SocketRedisConnection($socket);

        try {
            $connection->receive();
            self::fail('Expected ' . RedisConnectionException::class . ' to be thrown');
        } catch (RedisConnectionException $exception) {
            self::assertStringContainsString('Connection reset by peer', $exception->getMessage());
            self::assertInstanceOf(StreamException::class, $exception->getPrevious());
        }
    }
}
