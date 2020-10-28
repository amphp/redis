<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;

class DownTest extends AsyncTestCase
{
    public function test(): void
    {
        $this->expectException(SocketException::class);

        $redis = new Redis(new RemoteExecutor(Config::fromUri('tcp://127.0.0.1:25325')));
        $redis->ping();
    }
}
