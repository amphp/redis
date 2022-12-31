<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Parser\Parser;
use Amp\Redis\ParserException;
use Amp\Redis\QueryException;

final class RespParser extends Parser
{
    private const CRLF = "\r\n";

    private const TYPE_SIMPLE_STRING = '+';
    private const TYPE_ERROR = '-';
    private const TYPE_ARRAY = '*';
    private const TYPE_BULK_STRING = '$';
    private const TYPE_INTEGER = ':';

    /**
     * @param \Closure(RespPayload):void $push
     */
    public function __construct(\Closure $push)
    {
        parent::__construct(self::parser($push));
    }

    /**
     * @param \Closure(RespPayload):void $push
     *
     * @return \Generator<int, int|string, string, void>
     */
    private static function parser(\Closure $push): \Generator
    {
        while (true) {
            try {
                $value = self::parseValue(yield 1, yield self::CRLF);
                $push(new RespValue($value instanceof \Generator ? yield from $value : $value));
            } catch (QueryException $e) {
                $push(new RespError($e));
            }
        }
    }

    /**
     * @return int|string|\Generator<int, int|string, string, string|null|list<mixed>>
     */
    private static function parseValue(string $type, string $payload): \Generator|int|string
    {
        return match ($type) {
            self::TYPE_SIMPLE_STRING => $payload,
            self::TYPE_INTEGER => (int) $payload,
            self::TYPE_BULK_STRING => self::parseString((int) $payload),
            self::TYPE_ARRAY => self::parseArray((int) $payload),
            self::TYPE_ERROR => throw new QueryException($payload),
            default => throw new ParserException('Unknown resp data type: ' . $type),
        };
    }

    /**
     * @return \Generator<int, int|string, string, string|null>
     */
    private static function parseString(int $length): \Generator
    {
        if ($length < -1) {
            throw new ParserException('Invalid string length: ' . $length);
        }

        if ($length === -1) {
            return null;
        }

        $payload = match ($length) {
            0 => '',
            default => yield $length,
        };

        yield 2; // Remove trailing CRLF

        return $payload;
    }

    /**
     * @return \Generator<int, int|string, string, list<mixed>|null>
     */
    private static function parseArray(int $count): \Generator
    {
        if ($count < -1) {
            throw new ParserException('Invalid array length: ' . $count);
        }

        if ($count === -1) {
            return null;
        }

        $payload = [];
        for ($i = 0; $i < $count; $i++) {
            $value = self::parseValue(yield 1, yield self::CRLF);
            $payload[] = $value instanceof \Generator ? yield from $value : $value;
        }

        return $payload;
    }
}
