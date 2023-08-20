<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Cancellation;
use Amp\Redis\RedisException;

interface RedisChannelFactory
{
    /**
     * @throws RedisException
     */
    public function createChannel(?Cancellation $cancellation = null): RedisChannel;
}
