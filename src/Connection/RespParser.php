<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Parser\Parser;
use Amp\Redis\ParserException;

/**
 * @psalm-type ParserGeneratorType = \Generator<int, int|string, string, RespPayload>
 */
final class RespParser
{
    private const CRLF = "\r\n";

    private const TYPE_SIMPLE_STRING = '+';
    private const TYPE_ERROR = '-';
    private const TYPE_ARRAY = '*';
    private const TYPE_BULK_STRING = '$';
    private const TYPE_INTEGER = ':';

    private readonly Parser $parser;

    /**
     * @param \Closure(RespPayload):void $push
     */
    public function __construct(\Closure $push)
    {
        $this->parser = new Parser(self::parser($push));
    }

    public function push(string $data): void
    {
        $this->parser->push($data);
    }

    public function cancel(): void
    {
        $this->parser->cancel();
    }

    /**
     * @param \Closure(RespPayload):void $push
     *
     * @return \Generator<int, int|string, string, void>
     */
    private static function parser(\Closure $push): \Generator
    {
        while (true) {
            $push(yield from self::parseValue(yield 1, yield self::CRLF));
        }
    }

    /**
     * @return ParserGeneratorType
     */
    private static function parseValue(string $type, string $payload): \Generator
    {
        return match ($type) {
            self::TYPE_SIMPLE_STRING => new RespValue($payload),
            self::TYPE_INTEGER => new RespValue((int) $payload),
            self::TYPE_BULK_STRING => yield from self::parseString((int) $payload),
            self::TYPE_ARRAY => yield from self::parseArray((int) $payload),
            self::TYPE_ERROR => new RespError($payload),
            default => throw new ParserException('Unknown resp data type: ' . $type),
        };
    }

    /**
     * @return ParserGeneratorType
     */
    private static function parseString(int $length): \Generator
    {
        if ($length < -1) {
            throw new ParserException('Invalid string length: ' . $length);
        }

        if ($length === -1) {
            return new RespValue(null);
        }

        $payload = match ($length) {
            0 => '',
            default => yield $length,
        };

        yield 2; // Remove trailing CRLF

        return new RespValue($payload);
    }

    /**
     * @return ParserGeneratorType
     */
    private static function parseArray(int $count): \Generator
    {
        if ($count < -1) {
            throw new ParserException('Invalid array length: ' . $count);
        }

        if ($count === -1) {
            return new RespValue(null);
        }

        $payload = [];
        for ($i = 0; $i < $count; $i++) {
            $payload[] = (yield from self::parseValue(yield 1, yield self::CRLF))->unwrap();
        }

        return new RespValue($payload);
    }
}
