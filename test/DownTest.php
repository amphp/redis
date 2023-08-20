<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;

class DownTest extends AsyncTestCase
{
    public function test(): void
    {
        $this->expectException(RedisSocketException::class);

        $redis = new RedisCommands(createRedisClient('tcp://127.0.0.1:25325'));
        $redis->ping();
    }
}
