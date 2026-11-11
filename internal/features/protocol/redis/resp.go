package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	ErrInvalidRESP = errors.New("redis: invalid RESP syntax")
)

// ReadCommand parses a command array from a RESP reader.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, nil
	}

	if line[0] == '*' {
		// RESP Array format
		numArgs, err := strconv.Atoi(line[1:])
		if err != nil || numArgs < 0 {
			return nil, ErrInvalidRESP
		}
		args := make([]string, numArgs)
		for i := 0; i < numArgs; i++ {
			argLine, err := r.ReadString('\n')
			if err != nil {
				return nil, err
			}
			argLine = strings.TrimRight(argLine, "\r\n")
			if len(argLine) == 0 || argLine[0] != '$' {
				return nil, ErrInvalidRESP
			}
			argLen, err := strconv.Atoi(argLine[1:])
			if err != nil {
				return nil, ErrInvalidRESP
			}
			if argLen < 0 {
				args[i] = ""
				continue
			}

			buf := make([]byte, argLen+2) // data + \r\n
			_, err = io.ReadFull(r, buf)
			if err != nil {
				return nil, err
			}
			args[i] = string(buf[:argLen])
		}
		return args, nil
	}

	// Inline command format (e.g. "PING\r\n")
	parts := strings.Fields(line)
	return parts, nil
}

// EncodeSimpleString returns "+<str>\r\n".
func EncodeSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

// EncodeError returns "-ERR <msg>\r\n".
func EncodeError(msg string) []byte {
	return []byte("-ERR " + msg + "\r\n")
}

// EncodeInteger returns ":<n>\r\n".
func EncodeInteger(n int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", n))
}

// EncodeBulkString returns "$<len>\r\n<data>\r\n".
func EncodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

// EncodeNullBulkString returns "$-1\r\n".
func EncodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}

// EncodeArrayHeader returns "*<count>\r\n".
func EncodeArrayHeader(count int) []byte {
	return []byte(fmt.Sprintf("*%d\r\n", count))
}

// EncodeEmptyArray returns "*0\r\n".
func EncodeEmptyArray() []byte {
	return []byte("*0\r\n")
}

// EncodeNullArray returns "*-1\r\n" (null multi-bulk/nil array).
func EncodeNullArray() []byte {
	return []byte("*-1\r\n")
}

// EncodeStreamEntry formats a single stream entry [id, [field, val, ...]].
func EncodeStreamEntry(id string, fieldValues [][2]string) []byte {
	buf := new(bytes.Buffer)
	buf.WriteString("*2\r\n")
	buf.Write(EncodeBulkString(id))

	buf.WriteString(fmt.Sprintf("*%d\r\n", len(fieldValues)*2))
	for _, kv := range fieldValues {
		buf.Write(EncodeBulkString(kv[0]))
		buf.Write(EncodeBulkString(kv[1]))
	}

	return buf.Bytes()
}

