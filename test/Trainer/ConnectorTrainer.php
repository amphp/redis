<?php

namespace Amp\Redis\Trainer;

use Amp\CancellationToken;
use Amp\Failure;
use Amp\Promise;
use Amp\Socket\ConnectContext;
use Amp\Socket\Connector;
use Amp\Socket\SocketException;
use function Amp\Socket\connector;

final class ConnectorTrainer implements Connector
{
    /** @var Connector */
    private $connector;

    public function __construct()
    {
        $this->givenConnectIsNotIntercepted();
    }

    public function givenConnector(Connector $connector): void
    {
        $this->connector = $connector;
    }

    public function givenConnectFails(): void
    {
        $this->givenConnector(new class implements Connector {
            public function connect(
                string $uri,
                ?ConnectContext $context = null,
                ?CancellationToken $token = null
            ): Promise {
                return new Failure(new SocketException('Connect to ' . $uri . ' failed'));
            }
        });
    }

    public function givenConnectIsNotIntercepted(): void
    {
        $this->givenConnector(connector());
    }

    public function connect(string $uri, ?ConnectContext $context = null, ?CancellationToken $token = null): Promise
    {
        return $this->connector->connect($uri, $context, $token);
    }
}
