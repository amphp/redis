<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Redis\Connection\RedisConnectionException;

class DownTest extends AsyncTestCase
{
    public function test(): void
    {
        $this->expectException(RedisConnectionException::class);

        $redis = createRedisClient('tcp://127.0.0.1:25325');
        $redis->ping();
    }
}
