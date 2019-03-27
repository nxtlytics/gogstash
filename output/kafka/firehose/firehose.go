package firehose

import (
	"bytes"
	"encoding/binary"
)

func Encode(topic string, msg []byte) []byte {

	topicLen := uint8(len(topic))
	msgLen := uint32(len(msg))

	var buf bytes.Buffer
	buf.WriteByte(1)
	binary.Write(&buf, binary.LittleEndian, topicLen)
	binary.Write(&buf, binary.LittleEndian, msgLen)
	buf.WriteString(topic)
	buf.WriteByte(2)
	buf.Write(msg)

	return buf.Bytes()
}

func Decode(msg []byte) (string, []byte) {

	if msg[0] != 1 {
		return "", []byte{}
	}

	topicLen := uint8(msg[1])
	// msgLen := binary.LittleEndian.Uint32(msg[2:6])

	end := topicLen + 6

	topic := string(msg[6:end])
	msgStart := end + 1
	message := msg[msgStart:]

	return topic, message
}

func GetTopic(msg []byte) string {

	topicLen := uint8(msg[1])
	// msgLen := binary.LittleEndian.Uint32(msg[2:6])

	end := topicLen + 6

	topic := string(msg[6:end])
	return topic
}
