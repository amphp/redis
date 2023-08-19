<?php declare(strict_types=1);

namespace Amp\Redis\Sync;

use Amp\Redis\IntegrationTest;
use Amp\Redis\SocketRedisClientFactory;
use function Amp\async;
use function Amp\delay;

class RedisParcelTest extends IntegrationTest
{
    public function testUnwrapIsOfCorrectType(): void
    {
        $parcel = $this->createParcel(new \stdClass);
        self::assertInstanceOf(\stdClass::class, $parcel->unwrap());
    }

    public function testUnwrapIsEqual(): void
    {
        $object = new \stdClass;
        $parcel = $this->createParcel($object);
        self::assertEquals($object, $parcel->unwrap());
    }

    public function testSynchronized(): void
    {
        $parcel = $this->createParcel(0);

        $future1 = async(fn () =>$parcel->synchronized(function ($value): int {
            $this->assertSame(0, $value);
            delay(0.2);
            return 1;
        }));

        $future2 = async(fn () => $parcel->synchronized(function ($value): int {
            $this->assertSame(1, $value);
            delay(0.1);
            return 2;
        }));

        self::assertSame(1, $future1->await());
        self::assertSame(2, $future2->await());
    }

    protected function createParcel(mixed $value): RedisParcel
    {
        $clientFactory = new SocketRedisClientFactory($this->getUri());
        $mutex = new RedisMutex($clientFactory->createRedisClient());

        return RedisParcel::create($mutex, \bin2hex(\random_bytes(8)), $value);
    }
}
