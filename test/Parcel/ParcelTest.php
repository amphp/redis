<?php

namespace Amp\Redis\Parcel;

use Amp\Parallel\Test\Sync\AbstractParcelTest;
use Amp\Promise;
use Amp\Redis\Config;
use Amp\Redis\Mutex\Mutex;
use Amp\Redis\RemoteExecutorFactory;

class RedisParcelTest extends AbstractParcelTest
{
    const ID = __CLASS__;

    protected function createParcel($value): Promise
    {
        $config = Config::fromUri('redis://localhost');
        $executorFactory = new RemoteExecutorFactory($config);
        $mutex = new Mutex($executorFactory);

        return Parcel::create($executorFactory, $mutex, self::ID, $value);
    }
}
