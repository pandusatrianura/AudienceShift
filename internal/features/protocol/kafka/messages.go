package kafka

import (
	"bytes"
	"fmt"
	"time"
)

// Standard Kafka API Keys
const (
	ApiKeyProduce         int16 = 0
	ApiKeyFetch           int16 = 1
	ApiKeyListOffsets     int16 = 2
	ApiKeyMetadata        int16 = 3
	ApiKeyOffsetCommit    int16 = 8
	ApiKeyOffsetFetch     int16 = 9
	ApiKeyFindCoordinator int16 = 10
	ApiKeyJoinGroup       int16 = 11
	ApiKeyHeartbeat       int16 = 12
	ApiKeyLeaveGroup      int16 = 13
	ApiKeySyncGroup       int16 = 14
	ApiKeyDescribeGroups  int16 = 15
	ApiKeyListGroups      int16 = 16
	ApiKeyApiVersions     int16 = 18
)

// Standard Kafka Error Codes
const (
	ErrNone                     int16 = 0
	ErrOffsetOutOfRange         int16 = 1
	ErrUnknownTopicOrPartition  int16 = 3
	ErrNotLeaderOrFollower      int16 = 6
	ErrCoordinatorNotAvailable  int16 = 15
	ErrNotCoordinator           int16 = 16
	ErrIllegalGeneration        int16 = 22
	ErrUnsupportedVersion       int16 = 35
	ErrKafkaStorageError        int16 = 56 // KAFKA_STORAGE_ERROR: Disk error when accessing log/partition
)

// RequestHeader is the standard Kafka request header (v0-v1).
type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

// DecodeRequestHeader parses the Kafka request header from the decoder.
func DecodeRequestHeader(d *Decoder) (*RequestHeader, error) {
	apiKey, err := d.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("read ApiKey: %w", err)
	}
	apiVersion, err := d.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("read ApiVersion: %w", err)
	}
	corrId, err := d.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read CorrelationId: %w", err)
	}
	clientId, err := d.GetString()
	if err != nil {
		// ClientId is optional/can be empty
		clientId = ""
	}

	return &RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationId: corrId,
		ClientId:      clientId,
	}, nil
}

// SupportedApi holds the supported min/max version for a Kafka API key.
type SupportedApi struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

// ApiVersionsResponse represents the ApiVersions (Key 18) response.
type ApiVersionsResponse struct {
	CorrelationId int32
	ErrorCode     int16
	ApiKeys       []SupportedApi
}

// BuildApiVersionsResponse creates a default ApiVersions response advertising supported APIs.
func BuildApiVersionsResponse(correlationId int32) *ApiVersionsResponse {
	return &ApiVersionsResponse{
		CorrelationId: correlationId,
		ErrorCode:     ErrNone,
		ApiKeys: []SupportedApi{
			{ApiKey: ApiKeyProduce, MinVersion: 0, MaxVersion: 8},
			{ApiKey: ApiKeyFetch, MinVersion: 0, MaxVersion: 11},
			{ApiKey: ApiKeyListOffsets, MinVersion: 0, MaxVersion: 5},
			{ApiKey: ApiKeyMetadata, MinVersion: 0, MaxVersion: 5},
			{ApiKey: ApiKeyOffsetCommit, MinVersion: 0, MaxVersion: 8},
			{ApiKey: ApiKeyOffsetFetch, MinVersion: 0, MaxVersion: 6},
			{ApiKey: ApiKeyFindCoordinator, MinVersion: 0, MaxVersion: 2},
			{ApiKey: ApiKeyJoinGroup, MinVersion: 0, MaxVersion: 2},
			{ApiKey: ApiKeyHeartbeat, MinVersion: 0, MaxVersion: 2},
			{ApiKey: ApiKeyLeaveGroup, MinVersion: 0, MaxVersion: 2},
			{ApiKey: ApiKeySyncGroup, MinVersion: 0, MaxVersion: 2},
			{ApiKey: ApiKeyApiVersions, MinVersion: 0, MaxVersion: 0},
		},
	}
}

// Encode serializes the ApiVersions response into bytes.
func (r *ApiVersionsResponse) Encode(version ...int16) []byte {
	var v int16 = 0
	if len(version) > 0 {
		v = version[0]
	}
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	enc.PutInt16(r.ErrorCode)
	enc.PutInt32(int32(len(r.ApiKeys)))

	for _, api := range r.ApiKeys {
		enc.PutInt16(api.ApiKey)
		enc.PutInt16(api.MinVersion)
		enc.PutInt16(api.MaxVersion)
	}

	if v >= 1 {
		enc.PutInt32(0) // ThrottleTimeMs
	}

	return buf.Bytes()
}

// BrokerInfo represents broker metadata.
type BrokerInfo struct {
	NodeId int32
	Host   string
	Port   int32
}

// PartitionMetadataResponse represents partition metadata in a Metadata response.
type PartitionMetadataResponse struct {
	ErrorCode   int16
	PartitionId int32
	Leader      int32
	Replicas    []int32
	Isr         []int32
}

// TopicMetadataResponse represents topic metadata in a Metadata response.
type TopicMetadataResponse struct {
	ErrorCode  int16
	Topic      string
	IsInternal bool
	Partitions []PartitionMetadataResponse
}

// MetadataResponse represents the Metadata (Key 3) response.
type MetadataResponse struct {
	CorrelationId int32
	Brokers       []BrokerInfo
	Topics        []TopicMetadataResponse
}

// Encode serializes the MetadataResponse into bytes matching the requested API version.
func (r *MetadataResponse) Encode(version ...int16) []byte {
	var v int16 = 0
	if len(version) > 0 {
		v = version[0]
	}

	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)

	if v >= 3 {
		enc.PutInt32(0) // ThrottleTimeMs
	}

	// Brokers array
	enc.PutInt32(int32(len(r.Brokers)))
	for _, b := range r.Brokers {
		enc.PutInt32(b.NodeId)
		enc.PutString(b.Host)
		enc.PutInt32(b.Port)
		if v >= 1 {
			enc.PutNullableString(nil) // Rack (nullable string)
		}
	}

	if v >= 2 {
		clusterID := "pocketqueue"
		enc.PutNullableString(&clusterID) // ClusterId (nullable string)
	}

	if v >= 1 {
		enc.PutInt32(0) // ControllerId (int32)
	}

	// Topics array
	enc.PutInt32(int32(len(r.Topics)))
	for _, t := range r.Topics {
		enc.PutInt16(t.ErrorCode)
		enc.PutString(t.Topic)
		if v >= 1 {
			if t.IsInternal {
				enc.PutInt8(1)
			} else {
				enc.PutInt8(0)
			}
		}

		enc.PutInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			enc.PutInt16(p.ErrorCode)
			enc.PutInt32(p.PartitionId)
			enc.PutInt32(p.Leader)

			if v >= 7 {
				enc.PutInt32(0) // LeaderEpoch
			}

			// Replicas
			enc.PutInt32(int32(len(p.Replicas)))
			for _, rep := range p.Replicas {
				enc.PutInt32(rep)
			}

			// ISR
			enc.PutInt32(int32(len(p.Isr)))
			for _, isr := range p.Isr {
				enc.PutInt32(isr)
			}

			if v >= 5 {
				enc.PutInt32(0) // OfflineReplicas (count 0)
			}
		}
	}

	return buf.Bytes()
}

// FindCoordinatorResponse represents the FindCoordinator (Key 10) response.
type FindCoordinatorResponse struct {
	CorrelationId int32
	ErrorCode     int16
	NodeId        int32
	Host          string
	Port          int32
}

// Encode serializes FindCoordinatorResponse.
func (r *FindCoordinatorResponse) Encode(version ...int16) []byte {
	var v int16 = 0
	if len(version) > 0 {
		v = version[0]
	}

	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if v >= 1 {
		enc.PutInt32(0) // ThrottleTimeMs
	}
	enc.PutInt16(r.ErrorCode)
	if v >= 1 {
		enc.PutNullableString(nil) // ErrorMessage
	}
	enc.PutInt32(r.NodeId)
	enc.PutString(r.Host)
	enc.PutInt32(r.Port)

	return buf.Bytes()
}

// RecordItem represents a simple decoded record inside a Kafka batch.
type RecordItem struct {
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

// EncodeRecordBatchV2 encodes a set of records into a Kafka RecordBatch v2 byte slice.
func EncodeRecordBatchV2(baseOffset int64, records []RecordItem) []byte {
	if len(records) == 0 {
		return nil
	}

	// 1. Encode inner records first
	recordsBuf := new(bytes.Buffer)
	recEnc := NewEncoder(recordsBuf)

	nowMs := time.Now().UnixMilli()

	for i, r := range records {
		recBodyBuf := new(bytes.Buffer)
		bEnc := NewEncoder(recBodyBuf)

		bEnc.PutInt8(0)                  // attributes
		bEnc.PutVarint(0)                // timestamp delta
		bEnc.PutVarint(int64(i))         // offset delta

		if r.Key == nil {
			bEnc.PutVarint(-1)
		} else {
			bEnc.PutVarint(int64(len(r.Key)))
			bEnc.PutRaw(r.Key)
		}

		if r.Value == nil {
			bEnc.PutVarint(-1)
		} else {
			bEnc.PutVarint(int64(len(r.Value)))
			bEnc.PutRaw(r.Value)
		}

		// Headers
		bEnc.PutVarint(int64(len(r.Headers)))
		for k, v := range r.Headers {
			bEnc.PutVarint(int64(len(k)))
			bEnc.PutRaw([]byte(k))
			if v == nil {
				bEnc.PutVarint(-1)
			} else {
				bEnc.PutVarint(int64(len(v)))
				bEnc.PutRaw(v)
			}
		}

		// Record length (varint) + record bytes
		recEnc.PutVarint(int64(recBodyBuf.Len()))
		recEnc.PutRaw(recBodyBuf.Bytes())
	}

	// 2. Encode batch header + inner records to compute CRC
	// CRC covers from Attributes (byte 21) to end of batch
	crcDataBuf := new(bytes.Buffer)
	cEnc := NewEncoder(crcDataBuf)

	cEnc.PutInt16(0)                         // Attributes
	cEnc.PutInt32(int32(len(records) - 1))   // LastOffsetDelta
	cEnc.PutInt64(nowMs)                     // BaseTimestamp
	cEnc.PutInt64(nowMs)                     // MaxTimestamp
	cEnc.PutInt64(-1)                        // ProducerId
	cEnc.PutInt16(-1)                        // ProducerEpoch
	cEnc.PutInt32(-1)                        // BaseSequence
	cEnc.PutInt32(int32(len(records)))       // Records count
	cEnc.PutRaw(recordsBuf.Bytes())          // Records data

	crc := ChecksumCRC32(crcDataBuf.Bytes())

	// 3. Assemble full batch
	fullBuf := new(bytes.Buffer)
	fEnc := NewEncoder(fullBuf)

	fEnc.PutInt64(baseOffset)
	// BatchLength = 49 (header from PartitionLeaderEpoch to end of fixed header) + records length
	batchLength := int32(crcDataBuf.Len() + 9) // 4 (PartitionLeaderEpoch) + 1 (Magic) + 4 (CRC) = 9
	fEnc.PutInt32(batchLength)
	fEnc.PutInt32(0)                           // PartitionLeaderEpoch
	fEnc.PutInt8(2)                            // Magic = 2 (RecordBatch v2)
	fEnc.PutInt32(int32(crc))                  // CRC32-C
	fEnc.PutRaw(crcDataBuf.Bytes())

	return fullBuf.Bytes()
}

// DecodeRecordBatchV2 decodes records from a Kafka RecordBatch v2 or legacy payload.
func DecodeRecordBatchV2(data []byte) ([]RecordItem, error) {
	if len(data) < 12 {
		// Fallback: treat entire payload as single raw message
		return []RecordItem{{Value: data}}, nil
	}

	dec := NewDecoder(data)
	_, _ = dec.GetInt64() // baseOffset
	batchLength, err := dec.GetInt32()
	if err != nil || int(batchLength) > dec.Remaining()+4 {
		return []RecordItem{{Value: data}}, nil
	}

	_, _ = dec.GetInt32() // PartitionLeaderEpoch
	magic, err := dec.GetInt8()
	if err != nil || magic != 2 {
		// Not v2 RecordBatch; fallback to raw data
		return []RecordItem{{Value: data}}, nil
	}

	_, _ = dec.GetInt32() // CRC
	_, _ = dec.GetInt16() // Attributes
	_, _ = dec.GetInt32() // LastOffsetDelta
	_, _ = dec.GetInt64() // BaseTimestamp
	_, _ = dec.GetInt64() // MaxTimestamp
	_, _ = dec.GetInt64() // ProducerId
	_, _ = dec.GetInt16() // ProducerEpoch
	_, _ = dec.GetInt32() // BaseSequence

	recordsCount, err := dec.GetInt32()
	if err != nil || recordsCount <= 0 {
		return nil, nil
	}

	items := make([]RecordItem, 0, recordsCount)

	for i := 0; i < int(recordsCount); i++ {
		recLen, err := dec.GetVarint()
		if err != nil || recLen <= 0 {
			break
		}

		recSlice, err := dec.Slice(int(recLen))
		if err != nil {
			break
		}

		rDec := NewDecoder(recSlice)
		_, _ = rDec.GetInt8()   // Attributes
		_, _ = rDec.GetVarint() // TimestampDelta
		_, _ = rDec.GetVarint() // OffsetDelta

		keyLen, err := rDec.GetVarint()
		var key []byte
		if err == nil && keyLen > 0 {
			key, _ = rDec.Slice(int(keyLen))
		}

		valLen, err := rDec.GetVarint()
		var val []byte
		if err == nil && valLen > 0 {
			val, _ = rDec.Slice(int(valLen))
		}

		items = append(items, RecordItem{
			Key:   key,
			Value: val,
		})
	}

	if len(items) == 0 {
		return []RecordItem{{Value: data}}, nil
	}

	return items, nil
}


// OffsetCommitResponse represents the response to an OffsetCommit request.
type OffsetCommitResponse struct {
	CorrelationId int32
	Topics        []OffsetCommitTopicResponse
}

type OffsetCommitTopicResponse struct {
	Topic      string
	Partitions []OffsetCommitPartitionResponse
}

type OffsetCommitPartitionResponse struct {
	Partition int32
	ErrorCode int16
}

func (r *OffsetCommitResponse) Encode(version ...int16) []byte {
	var v int16 = 0
	if len(version) > 0 {
		v = version[0]
	}

	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if v >= 3 {
		enc.PutInt32(0) // ThrottleTimeMs
	}
	enc.PutInt32(int32(len(r.Topics)))
	for _, t := range r.Topics {
		enc.PutString(t.Topic)
		enc.PutInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			enc.PutInt32(p.Partition)
			enc.PutInt16(p.ErrorCode)
		}
	}
	return buf.Bytes()
}

// OffsetFetchResponse represents the response to an OffsetFetch request.
type OffsetFetchResponse struct {
	CorrelationId int32
	Topics        []OffsetFetchTopicResponse
}

type OffsetFetchTopicResponse struct {
	Topic      string
	Partitions []OffsetFetchPartitionResponse
}

type OffsetFetchPartitionResponse struct {
	Partition int32
	Offset    int64
	Metadata  string
	ErrorCode int16
}

func (r *OffsetFetchResponse) Encode(version ...int16) []byte {
	var v int16 = 0
	if len(version) > 0 {
		v = version[0]
	}

	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if v >= 3 {
		enc.PutInt32(0) // ThrottleTimeMs
	}
	enc.PutInt32(int32(len(r.Topics)))
	for _, t := range r.Topics {
		enc.PutString(t.Topic)
		enc.PutInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			enc.PutInt32(p.Partition)
			enc.PutInt64(p.Offset)
			if v >= 5 {
				enc.PutInt32(-1) // LeaderEpoch = -1
			}
			enc.PutString(p.Metadata)
			enc.PutInt16(p.ErrorCode)
		}
	}
	if v >= 2 {
		enc.PutInt16(ErrNone) // Top-level ErrorCode
	}
	return buf.Bytes()
}

// ListOffsetsResponse represents partition watermarks response.
type ListOffsetsResponse struct {
	CorrelationId int32
	Topics        []ListOffsetsTopicResponse
}

type ListOffsetsTopicResponse struct {
	Topic      string
	Partitions []ListOffsetsPartitionResponse
}

type ListOffsetsPartitionResponse struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

func (r *ListOffsetsResponse) Encode(version ...int16) []byte {
	var v int16 = 0
	if len(version) > 0 {
		v = version[0]
	}

	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if v >= 2 {
		enc.PutInt32(0) // ThrottleTimeMs
	}

	enc.PutInt32(int32(len(r.Topics)))
	for _, t := range r.Topics {
		enc.PutString(t.Topic)
		enc.PutInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			enc.PutInt32(p.Partition)
			enc.PutInt16(p.ErrorCode)
			if v == 0 {
				enc.PutInt32(1) // 1 offset
				enc.PutInt64(p.Offset)
			} else {
				enc.PutInt64(p.Timestamp)
				enc.PutInt64(p.Offset)
				if v >= 4 {
					enc.PutInt32(0) // LeaderEpoch
				}
			}
		}
	}
	return buf.Bytes()
}

// ProduceResponse represents a Produce request response.
type ProduceResponse struct {
	CorrelationId int32
	Topics        []ProduceTopicResponse
	ThrottleTime  int32
}

type ProduceTopicResponse struct {
	Topic      string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Partition      int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}

func (r *ProduceResponse) Encode() []byte {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	enc.PutInt32(int32(len(r.Topics)))
	for _, t := range r.Topics {
		enc.PutString(t.Topic)
		enc.PutInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			enc.PutInt32(p.Partition)
			enc.PutInt16(p.ErrorCode)
			enc.PutInt64(p.BaseOffset)
			enc.PutInt64(p.LogAppendTime)
			enc.PutInt64(p.LogStartOffset)
		}
	}
	enc.PutInt32(r.ThrottleTime)
	return buf.Bytes()
}

// FetchResponse represents a Fetch request response.
type FetchResponse struct {
	CorrelationId int32
	ThrottleTime  int32
	Topics        []FetchTopicResponse
}

type FetchTopicResponse struct {
	Topic      string
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	Partition           int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	RecordBatch         []byte
}

func (r *FetchResponse) Encode(version ...int16) []byte {
	var v int16 = 0
	if len(version) > 0 {
		v = version[0]
	}

	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if v >= 1 {
		enc.PutInt32(r.ThrottleTime)
	}
	if v >= 7 {
		enc.PutInt16(0) // ErrorCode
		enc.PutInt32(0) // SessionID
	}

	enc.PutInt32(int32(len(r.Topics)))
	for _, t := range r.Topics {
		enc.PutString(t.Topic)
		enc.PutInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			enc.PutInt32(p.Partition)
			enc.PutInt16(p.ErrorCode)
			enc.PutInt64(p.HighWatermark)
			if v >= 4 {
				enc.PutInt64(p.LastStableOffset)
				enc.PutInt64(p.LogStartOffset)
				enc.PutInt32(0) // Aborted transactions count = 0
			}
			if v >= 11 {
				enc.PutInt32(-1) // PreferredReadReplica = -1
			}
			// Put record batch bytes (length prefix + bytes)
			enc.PutBytes(p.RecordBatch)
		}
	}
	return buf.Bytes()
}

// GroupProtocol holds consumer group protocol name and metadata.
type GroupProtocol struct {
	ProtocolName     string
	ProtocolMetadata []byte
}

// JoinGroupRequest parses a JoinGroup request.
type JoinGroupRequest struct {
	GroupId          string
	SessionTimeout   int32
	RebalanceTimeout int32
	MemberId         string
	ProtocolType     string
	GroupProtocols   []GroupProtocol
}

// DecodeJoinGroupRequest parses the JoinGroup body based on API version.
func DecodeJoinGroupRequest(d *Decoder, apiVersion int16) (*JoinGroupRequest, error) {
	gid, err := d.GetString()
	if err != nil {
		return nil, fmt.Errorf("read GroupId: %w", err)
	}
	sessionTimeout, err := d.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read SessionTimeout: %w", err)
	}
	var rebalanceTimeout int32
	if apiVersion >= 1 {
		rebalanceTimeout, err = d.GetInt32()
		if err != nil {
			return nil, fmt.Errorf("read RebalanceTimeout: %w", err)
		}
	}
	memberId, err := d.GetString()
	if err != nil {
		return nil, fmt.Errorf("read MemberId: %w", err)
	}
	protocolType, err := d.GetString()
	if err != nil {
		return nil, fmt.Errorf("read ProtocolType: %w", err)
	}
	protoCount, err := d.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read GroupProtocols count: %w", err)
	}
	protocols := make([]GroupProtocol, 0, protoCount)
	for i := 0; i < int(protoCount); i++ {
		pName, err := d.GetString()
		if err != nil {
			return nil, fmt.Errorf("read ProtocolName: %w", err)
		}
		pMeta, err := d.GetBytes()
		if err != nil {
			return nil, fmt.Errorf("read ProtocolMetadata: %w", err)
		}
		protocols = append(protocols, GroupProtocol{
			ProtocolName:     pName,
			ProtocolMetadata: pMeta,
		})
	}
	return &JoinGroupRequest{
		GroupId:          gid,
		SessionTimeout:   sessionTimeout,
		RebalanceTimeout: rebalanceTimeout,
		MemberId:         memberId,
		ProtocolType:     protocolType,
		GroupProtocols:   protocols,
	}, nil
}

// JoinGroupMember represents a member returned to group leader.
type JoinGroupMember struct {
	MemberId       string
	MemberMetadata []byte
}

// JoinGroupResponse represents the response to JoinGroup request.
type JoinGroupResponse struct {
	CorrelationId int32
	ThrottleTime  int32
	ErrorCode     int16
	GenerationId  int32
	GroupProtocol string
	LeaderId      string
	MemberId      string
	Members       []JoinGroupMember
}

// Encode serializes the JoinGroup response into bytes.
func (r *JoinGroupResponse) Encode(apiVersion int16) []byte {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if apiVersion >= 2 {
		enc.PutInt32(r.ThrottleTime)
	}
	enc.PutInt16(r.ErrorCode)
	enc.PutInt32(r.GenerationId)
	enc.PutString(r.GroupProtocol)
	enc.PutString(r.LeaderId)
	enc.PutString(r.MemberId)

	enc.PutInt32(int32(len(r.Members)))
	for _, m := range r.Members {
		enc.PutString(m.MemberId)
		enc.PutBytes(m.MemberMetadata)
	}

	return buf.Bytes()
}

// GroupMemberAssignment maps a group member to assigned partition bytes.
type GroupMemberAssignment struct {
	MemberId   string
	Assignment []byte
}

// SyncGroupRequest represents a SyncGroup request.
type SyncGroupRequest struct {
	GroupId         string
	GenerationId    int32
	MemberId        string
	GroupAssignment []GroupMemberAssignment
}

// DecodeSyncGroupRequest parses the SyncGroup request.
func DecodeSyncGroupRequest(d *Decoder, apiVersion int16) (*SyncGroupRequest, error) {
	gid, err := d.GetString()
	if err != nil {
		return nil, fmt.Errorf("read GroupId: %w", err)
	}
	genId, err := d.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read GenerationId: %w", err)
	}
	memberId, err := d.GetString()
	if err != nil {
		return nil, fmt.Errorf("read MemberId: %w", err)
	}
	assignCount, err := d.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read GroupAssignment count: %w", err)
	}
	assignments := make([]GroupMemberAssignment, 0, assignCount)
	for i := 0; i < int(assignCount); i++ {
		mId, err := d.GetString()
		if err != nil {
			return nil, fmt.Errorf("read MemberId in assignment: %w", err)
		}
		assignBytes, err := d.GetBytes()
		if err != nil {
			return nil, fmt.Errorf("read MemberAssignment bytes: %w", err)
		}
		assignments = append(assignments, GroupMemberAssignment{
			MemberId:   mId,
			Assignment: assignBytes,
		})
	}
	return &SyncGroupRequest{
		GroupId:         gid,
		GenerationId:    genId,
		MemberId:        memberId,
		GroupAssignment: assignments,
	}, nil
}

// SyncGroupResponse represents the response to SyncGroup request.
type SyncGroupResponse struct {
	CorrelationId    int32
	ThrottleTime     int32
	ErrorCode        int16
	MemberAssignment []byte
}

// Encode serializes the SyncGroup response.
func (r *SyncGroupResponse) Encode(apiVersion int16) []byte {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if apiVersion >= 1 {
		enc.PutInt32(r.ThrottleTime)
	}
	enc.PutInt16(r.ErrorCode)
	enc.PutBytes(r.MemberAssignment)

	return buf.Bytes()
}

// HeartbeatResponse represents the Heartbeat response.
type HeartbeatResponse struct {
	CorrelationId int32
	ThrottleTime  int32
	ErrorCode     int16
}

// Encode serializes Heartbeat response.
func (r *HeartbeatResponse) Encode(apiVersion int16) []byte {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if apiVersion >= 1 {
		enc.PutInt32(r.ThrottleTime)
	}
	enc.PutInt16(r.ErrorCode)

	return buf.Bytes()
}

// LeaveGroupResponse represents the LeaveGroup response.
type LeaveGroupResponse struct {
	CorrelationId int32
	ThrottleTime  int32
	ErrorCode     int16
}

// Encode serializes LeaveGroup response.
func (r *LeaveGroupResponse) Encode(apiVersion int16) []byte {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)

	enc.PutInt32(r.CorrelationId)
	if apiVersion >= 1 {
		enc.PutInt32(r.ThrottleTime)
	}
	enc.PutInt16(r.ErrorCode)

	return buf.Bytes()
}

// BuildDefaultMemberAssignment builds a standard consumer protocol assignment payload for given topics.
func BuildDefaultMemberAssignment(topics []string, partitionsPerTopic int32) []byte {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.PutInt16(0) // protocol version 0
	enc.PutInt32(int32(len(topics)))
	for _, t := range topics {
		enc.PutString(t)
		enc.PutInt32(partitionsPerTopic)
		for p := int32(0); p < partitionsPerTopic; p++ {
			enc.PutInt32(p)
		}
	}
	enc.PutBytes(nil) // UserData
	return buf.Bytes()
}
