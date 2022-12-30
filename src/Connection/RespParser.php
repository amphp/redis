<?php

namespace Amp\Redis\Connection;

use Amp\Parser\Parser;
use Amp\Pipeline\Queue;
use Amp\Redis\ParserException;
use Amp\Redis\QueryException;

/**
 * @psalm-import-type RedisValue from RespPayload
 */
final class RespParser extends Parser
{
    private const CRLF = "\r\n";

    private const TYPE_SIMPLE_STRING = '+';
    private const TYPE_ERROR = '-';
    private const TYPE_ARRAY = '*';
    private const TYPE_BULK_STRING = '$';
    private const TYPE_INTEGER = ':';

    public function __construct(Queue $queue)
    {
        parent::__construct(self::parser($queue));
    }

    /**
     * @return \Generator<int, int|string, string, void>
     */
    private static function parser(Queue $queue): \Generator
    {
        while (true) {
            try {
                $value = self::parseValue(yield 1, yield self::CRLF);
                $queue->push(new RespValue($value instanceof \Generator ? yield from $value : $value));
            } catch (QueryException $e) {
                $queue->push(new RespError($e));
            }
        }
    }

    /**
     * @return string|int|null|\Generator<int, int|string, string, RedisValue>
     */
    private static function parseValue(string $type, string $payload): \Generator|string|int|null
    {
        switch ($type) {
            case self::TYPE_SIMPLE_STRING:
                return $payload;

            case self::TYPE_INTEGER:
                return (int) $payload;

            case self::TYPE_BULK_STRING:
                $length = (int) $payload;

                if ($length < -1) {
                    throw new ParserException('Invalid string length: ' . $length);
                }

                if ($length === -1) {
                    return null;
                }

                return self::parseString($length);

            case self::TYPE_ARRAY:
                $count = (int) $payload;

                if ($count < -1) {
                    throw new ParserException('Invalid array length: ' . $count);
                }

                if ($count === -1) {
                    return null;
                }

                return self::parseArray($count);

            case self::TYPE_ERROR:
                throw new QueryException($payload);

            default:
                throw new ParserException('Unknown resp data type: ' . $type);
        }
    }

    /**
     * @return \Generator<int, int|string, string, string>
     */
    private static function parseString(int $length): \Generator
    {
        $payload = match ($length) {
            0 => '',
            default => yield $length,
        };

        yield self::CRLF;

        return $payload;
    }

    /**
     * @return \Generator<int, int|string, string, list<mixed>>
     */
    private static function parseArray(int $count): \Generator
    {
        $payload = [];
        for ($i = 0; $i < $count; $i++) {
            $value = self::parseValue(yield 1, yield self::CRLF);
            $payload[] = $value instanceof \Generator ? yield from $value : $value;
        }

        return $payload;
    }
}
