<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Cancellation;
use Amp\Redis\RedisConfig;
use Amp\Redis\RedisException;
use Amp\Socket\ConnectContext;

interface RedisConnector
{
    /**
     * @throws RedisException
     */
    public function connect(
        RedisConfig $config,
        ?ConnectContext $context = null,
        ?Cancellation $cancellation = null,
    ): RespChannel;
}
