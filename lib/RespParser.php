<?php

namespace Amphp\Redis;

class RespParser {
	const CRLF = "\r\n";

	private $responseCallback;
	private $buffer;
	private $bufferLength;
	private $arrayResponse;
	private $arrayLengths;

	public function __construct (callable $responseCallback) {
		$this->responseCallback = $responseCallback;
		$this->buffer = "";
		$this->bufferLength = 0;
		$this->arrayResponse = null;
		$this->arrayLengths = [];
	}

	public function append ($str) {
		$this->buffer .= $str;
		$this->bufferLength += strlen($str);

		while ($this->tryParse()) ;
	}

	private function tryParse () {
		if ($this->bufferLength === 0) {
			return false;
		}

		$type = $this->buffer[0];
		$pos = strpos($this->buffer, self::CRLF);

		if ($pos === false) {
			return false;
		}

		switch ($type) {
			case Resp::TYPE_SIMPLE_STRING:
				$this->onResponse($type, substr($this->buffer, 1, $pos - 1));
				$this->buffer = substr($this->buffer, $pos + 2);
				return true;

			case Resp::TYPE_ERROR:
				$this->onResponse($type, substr($this->buffer, 1, $pos - 1));
				$this->buffer = substr($this->buffer, $pos + 2);
				return true;

			case Resp::TYPE_INTEGER:
				$this->onResponse($type, (int) substr($this->buffer, 1, $pos - 1));
				$this->buffer = substr($this->buffer, $pos + 2);
				return true;

			case Resp::TYPE_BULK_STRING:
				$length = (int) substr($this->buffer, 1, $pos);

				if ($this->bufferLength < $pos + $length + 4) {
					return false;
				}

				$this->onResponse($type, substr($this->buffer, $pos + 2, $length));
				$this->buffer = substr($this->buffer, $pos + $length + 4);
				return true;

			case Resp::TYPE_ARRAY:
				$this->onResponse($type, (int) substr($this->buffer, 1, $pos - 1));
				$this->buffer = substr($this->buffer, $pos + 2);
				return true;

			default:
				return false; // throw exception?
		}
	}

	private function onResponse ($type, $payload) {
		if($this->arrayResponse !== null) {
			$arr = &$this->arrayResponse;
			$level = 1;

			while($level++ < sizeof($this->arrayLengths)) {
				end($arr);
				$arr = &$arr[key($arr)];
			}
		}

		if(isset($arr)) {
			$size = sizeof($arr);

			if($type === Resp::TYPE_ARRAY) {
				if($payload >= 0) {
					$this->arrayLengths[] = $payload;
					$arr[] = [];
					$size = 0;
				} else {
					$arr[] = null;
					$size++;
				}
			} else {
				$arr[] = $payload;
				$size++;
			}

			while($size === end($this->arrayLengths)) {
				array_pop($this->arrayLengths);

				$a = &$this->arrayResponse;
				$level = 1;

				while($level++ < sizeof($this->arrayLengths)) {
					end($a);
					$a = &$a[key($a)];
				}

				$size = sizeof($a);
			}

			if(sizeof($this->arrayLengths) === 0) {
				$this->on($this->arrayResponse);
				$this->arrayResponse = null;
			}
		} else if($type === Resp::TYPE_ARRAY) {
			if($payload > 0) {
				$this->arrayLengths[] = $payload;
				$this->arrayResponse = [];
			} else if($payload === 0) {
				$this->on([]);
			} else {
				$this->on(null);
			}
		} else {
			$this->on($payload);
		}
	}

	private function on($payload) {
		call_user_func($this->responseCallback, $payload);
	}
}
