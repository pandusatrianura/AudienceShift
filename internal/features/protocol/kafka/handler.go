package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// Handler processes Kafka requests and delegates to BrokerService.
type Handler struct {
	broker     *broker.Service
	advertisedHost string
	advertisedPort int32
	logger     *slog.Logger
}

// NewHandler creates a new Kafka protocol request handler.
func NewHandler(b *broker.Service, host string, port int32, l *slog.Logger) *Handler {
	if l == nil {
		l = slog.Default()
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return &Handler{
		broker:         b,
		advertisedHost: host,
		advertisedPort: port,
		logger:         l,
	}
}

// SetAdvertisedPort dynamically updates the advertised port (e.g. when bound to an ephemeral port).
func (h *Handler) SetAdvertisedPort(port int32) {
	h.advertisedPort = port
}

// HandleRequest routes a Kafka request to the appropriate sub-handler.
func (h *Handler) HandleRequest(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	switch header.ApiKey {
	case ApiKeyApiVersions:
		return h.handleApiVersions(header)
	case ApiKeyMetadata:
		return h.handleMetadata(ctx, header, body)
	case ApiKeyFindCoordinator:
		return h.handleFindCoordinator(header, body)
	case ApiKeyProduce:
		return h.handleProduce(ctx, header, body)
	case ApiKeyFetch:
		return h.handleFetch(ctx, header, body)
	case ApiKeyListOffsets:
		return h.handleListOffsets(ctx, header, body)
	case ApiKeyOffsetCommit:
		return h.handleOffsetCommit(ctx, header, body)
	case ApiKeyOffsetFetch:
		return h.handleOffsetFetch(ctx, header, body)
	case ApiKeyJoinGroup:
		return h.handleJoinGroup(ctx, header, body)
	case ApiKeySyncGroup:
		return h.handleSyncGroup(ctx, header, body)
	case ApiKeyHeartbeat:
		return h.handleHeartbeat(header, body)
	case ApiKeyLeaveGroup:
		return h.handleLeaveGroup(header, body)
	default:
		h.logger.WarnContext(ctx, "unsupported kafka api key", slog.Int("api_key", int(header.ApiKey)))
		// Return empty response with correlation ID
		resp := BuildApiVersionsResponse(header.CorrelationId)
		resp.ErrorCode = ErrUnsupportedVersion
		return resp.Encode(), nil
	}
}

func (h *Handler) handleApiVersions(header *RequestHeader) ([]byte, error) {
	resp := BuildApiVersionsResponse(header.CorrelationId)
	return resp.Encode(0), nil
}

func (h *Handler) handleFindCoordinator(header *RequestHeader, body []byte) ([]byte, error) {
	resp := FindCoordinatorResponse{
		CorrelationId: header.CorrelationId,
		ErrorCode:     ErrNone,
		NodeId:        0,
		Host:          h.advertisedHost,
		Port:          h.advertisedPort,
	}
	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleMetadata(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)
	topicsCount, err := dec.GetInt32()

	var requestedTopics []string
	if err == nil && topicsCount > 0 {
		for i := 0; i < int(topicsCount); i++ {
			tName, err := dec.GetString()
			if err == nil && tName != "" {
				requestedTopics = append(requestedTopics, tName)
			}
		}
	}

	// If no specific topics requested, list all
	if len(requestedTopics) == 0 {
		allTopics, _ := h.broker.ListTopics(ctx)
		for t := range allTopics {
			requestedTopics = append(requestedTopics, t)
		}
	}

	topicResponses := make([]TopicMetadataResponse, 0, len(requestedTopics))
	for _, tName := range requestedTopics {
		h.broker.EnsureTopic(tName, 1)
		meta, err := h.broker.GetTopicMetadata(ctx, tName)
		if err != nil {
			topicResponses = append(topicResponses, TopicMetadataResponse{
				ErrorCode: ErrUnknownTopicOrPartition,
				Topic:     tName,
			})
			continue
		}

		partResponses := make([]PartitionMetadataResponse, len(meta.Partitions))
		for i, p := range meta.Partitions {
			partResponses[i] = PartitionMetadataResponse{
				ErrorCode:   ErrNone,
				PartitionId: p.ID,
				Leader:      0,
				Replicas:    []int32{0},
				Isr:         []int32{0},
			}
		}

		topicResponses = append(topicResponses, TopicMetadataResponse{
			ErrorCode:  ErrNone,
			Topic:      tName,
			Partitions: partResponses,
		})
	}

	resp := MetadataResponse{
		CorrelationId: header.CorrelationId,
		Brokers: []BrokerInfo{
			{NodeId: 0, Host: h.advertisedHost, Port: h.advertisedPort},
		},
		Topics: topicResponses,
	}

	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleProduce(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)

	// TransactionalId (v3+)
	if header.ApiVersion >= 3 {
		_, _ = dec.GetNullableString()
	}
	acks, _ := dec.GetInt16() // Acks (0 = No response expected, 1 = leader ack, -1 = all replicas)
	_, _ = dec.GetInt32()     // TimeoutMs

	topicsCount, err := dec.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read produce topics count: %w", err)
	}

	topicResponses := make([]ProduceTopicResponse, 0, topicsCount)

	for i := 0; i < int(topicsCount); i++ {
		topicName, _ := dec.GetString()
		partsCount, _ := dec.GetInt32()
		partResponses := make([]ProducePartitionResponse, 0, partsCount)

		for j := 0; j < int(partsCount); j++ {
			partID, _ := dec.GetInt32()
			recordSetBytes, _ := dec.GetBytes()

			items, _ := DecodeRecordBatchV2(recordSetBytes)
			var baseOffset int64 = -1
			var lastErr error

			for _, item := range items {
				rec, err := h.broker.Produce(ctx, topicName, partID, item.Key, item.Value, nil)
				if err != nil {
					lastErr = err
				} else if baseOffset == -1 {
					baseOffset = rec.Offset
				}
			}

			if baseOffset == -1 {
				baseOffset = 0
			}

			errCode := ErrNone
			if lastErr != nil {
				if errors.Is(lastErr, storage.ErrWALFileSizeLimitExceeded) {
					errCode = ErrKafkaStorageError
				} else if errors.Is(lastErr, broker.ErrPartitionLimitExceeded) {
					errCode = ErrUnknownTopicOrPartition
				} else {
					errCode = ErrUnknownTopicOrPartition
				}
			}

			partResponses = append(partResponses, ProducePartitionResponse{
				Partition:      partID,
				ErrorCode:      errCode,
				BaseOffset:     baseOffset,
				LogAppendTime:  -1,
				LogStartOffset: 0,
			})
		}

		topicResponses = append(topicResponses, ProduceTopicResponse{
			Topic:      topicName,
			Partitions: partResponses,
		})
	}

	// For acks == 0, Kafka clients do not expect any response over the wire
	if acks == 0 {
		return nil, nil
	}

	resp := ProduceResponse{
		CorrelationId: header.CorrelationId,
		Topics:        topicResponses,
		ThrottleTime:  0,
	}

	return resp.Encode(), nil
}

func (h *Handler) handleFetch(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)

	_, _ = dec.GetInt32() // ReplicaId
	_, _ = dec.GetInt32() // MaxWaitMs
	_, _ = dec.GetInt32() // MinBytes

	if header.ApiVersion >= 3 {
		_, _ = dec.GetInt32() // MaxBytes
	}
	if header.ApiVersion >= 4 {
		_, _ = dec.GetInt8() // IsolationLevel
	}
	if header.ApiVersion >= 7 {
		_, _ = dec.GetInt32() // SessionId
		_, _ = dec.GetInt32() // SessionEpoch
	}

	topicsCount, err := dec.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read fetch topics count: %w", err)
	}

	topicResponses := make([]FetchTopicResponse, 0, topicsCount)

	for i := 0; i < int(topicsCount); i++ {
		topicName, _ := dec.GetString()
		partsCount, _ := dec.GetInt32()
		partResponses := make([]FetchPartitionResponse, 0, partsCount)

		for j := 0; j < int(partsCount); j++ {
			partID, _ := dec.GetInt32()
			fetchOffset, _ := dec.GetInt64()

			if header.ApiVersion >= 5 {
				_, _ = dec.GetInt64() // LogStartOffset
			}
			_, _ = dec.GetInt32() // MaxBytes

			records, _ := h.broker.Fetch(ctx, topicName, partID, fetchOffset, 500)
			meta, _ := h.broker.GetTopicMetadata(ctx, topicName)

			var hw int64 = 0
			if meta != nil && int(partID) < len(meta.Partitions) {
				hw = meta.Partitions[partID].Latest
			}

			var batchBytes []byte
			if len(records) > 0 {
				items := make([]RecordItem, len(records))
				for idx, r := range records {
					items[idx] = RecordItem{Key: r.Key, Value: r.Value}
				}
				batchBytes = EncodeRecordBatchV2(records[0].Offset, items)
			}

			partResponses = append(partResponses, FetchPartitionResponse{
				Partition:        partID,
				ErrorCode:        ErrNone,
				HighWatermark:    hw,
				LastStableOffset: hw,
				RecordBatch:      batchBytes,
			})
		}

		topicResponses = append(topicResponses, FetchTopicResponse{
			Topic:      topicName,
			Partitions: partResponses,
		})
	}

	resp := FetchResponse{
		CorrelationId: header.CorrelationId,
		ThrottleTime:  0,
		Topics:        topicResponses,
	}

	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleListOffsets(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)
	_, _ = dec.GetInt32() // ReplicaId
	if header.ApiVersion >= 2 {
		_, _ = dec.GetInt8() // IsolationLevel
	}

	topicsCount, err := dec.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read list offsets topics count: %w", err)
	}

	topicResponses := make([]ListOffsetsTopicResponse, 0, topicsCount)

	for i := 0; i < int(topicsCount); i++ {
		topicName, _ := dec.GetString()
		partsCount, _ := dec.GetInt32()
		partResponses := make([]ListOffsetsPartitionResponse, 0, partsCount)

		for j := 0; j < int(partsCount); j++ {
			partID, _ := dec.GetInt32()
			if header.ApiVersion >= 4 {
				_, _ = dec.GetInt32() // currentLeaderEpoch
			}
			targetTime, _ := dec.GetInt64()

			meta, _ := h.broker.GetTopicMetadata(ctx, topicName)
			var offset int64 = 0
			if meta != nil && int(partID) < len(meta.Partitions) {
				if targetTime == -2 { // Earliest
					offset = meta.Partitions[partID].Earliest
				} else { // Latest (-1)
					offset = meta.Partitions[partID].Latest
				}
			}

			partResponses = append(partResponses, ListOffsetsPartitionResponse{
				Partition: partID,
				ErrorCode: ErrNone,
				Timestamp: -1,
				Offset:    offset,
			})
		}

		topicResponses = append(topicResponses, ListOffsetsTopicResponse{
			Topic:      topicName,
			Partitions: partResponses,
		})
	}

	resp := ListOffsetsResponse{
		CorrelationId: header.CorrelationId,
		Topics:        topicResponses,
	}

	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleOffsetCommit(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)
	groupID, _ := dec.GetString()
	if header.ApiVersion >= 1 {
		_, _ = dec.GetInt32() // GenerationId
		_, _ = dec.GetString() // MemberId
	}

	topicsCount, err := dec.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read offset commit topics count: %w", err)
	}

	topicResponses := make([]OffsetCommitTopicResponse, 0, topicsCount)

	for i := 0; i < int(topicsCount); i++ {
		topicName, _ := dec.GetString()
		partsCount, _ := dec.GetInt32()
		partResponses := make([]OffsetCommitPartitionResponse, 0, partsCount)

		for j := 0; j < int(partsCount); j++ {
			partID, _ := dec.GetInt32()
			committedOffset, _ := dec.GetInt64()
			_, _ = dec.GetString() // Metadata

			_ = h.broker.CommitOffset(ctx, groupID, topicName, partID, committedOffset)

			partResponses = append(partResponses, OffsetCommitPartitionResponse{
				Partition: partID,
				ErrorCode: ErrNone,
			})
		}

		topicResponses = append(topicResponses, OffsetCommitTopicResponse{
			Topic:      topicName,
			Partitions: partResponses,
		})
	}

	resp := OffsetCommitResponse{
		CorrelationId: header.CorrelationId,
		Topics:        topicResponses,
	}

	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleOffsetFetch(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)
	groupID, _ := dec.GetString()

	topicsCount, err := dec.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("read offset fetch topics count: %w", err)
	}

	topicResponses := make([]OffsetFetchTopicResponse, 0, topicsCount)

	for i := 0; i < int(topicsCount); i++ {
		topicName, _ := dec.GetString()
		partsCount, _ := dec.GetInt32()
		partResponses := make([]OffsetFetchPartitionResponse, 0, partsCount)

		for j := 0; j < int(partsCount); j++ {
			partID, _ := dec.GetInt32()
			offset, _ := h.broker.GetOffset(ctx, groupID, topicName, partID)

			partResponses = append(partResponses, OffsetFetchPartitionResponse{
				Partition: partID,
				Offset:    offset,
				Metadata:  "",
				ErrorCode: ErrNone,
			})
		}

		topicResponses = append(topicResponses, OffsetFetchTopicResponse{
			Topic:      topicName,
			Partitions: partResponses,
		})
	}

	resp := OffsetFetchResponse{
		CorrelationId: header.CorrelationId,
		Topics:        topicResponses,
	}

	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleJoinGroup(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)
	req, err := DecodeJoinGroupRequest(dec, header.ApiVersion)
	if err != nil {
		h.logger.WarnContext(ctx, "failed to decode join group request", slog.Any("error", err))
		resp := &JoinGroupResponse{
			CorrelationId: header.CorrelationId,
			ErrorCode:     ErrUnknownTopicOrPartition,
		}
		return resp.Encode(header.ApiVersion), nil
	}

	memberID := req.MemberId
	if memberID == "" {
		if header.ClientId != "" {
			memberID = fmt.Sprintf("%s-pq-member", header.ClientId)
		} else {
			memberID = "pq-member-1"
		}
	}

	chosenProtocol := "roundrobin"
	var chosenMetadata []byte
	if len(req.GroupProtocols) > 0 {
		chosenProtocol = req.GroupProtocols[0].ProtocolName
		chosenMetadata = req.GroupProtocols[0].ProtocolMetadata
	}

	resp := &JoinGroupResponse{
		CorrelationId: header.CorrelationId,
		ThrottleTime:  0,
		ErrorCode:     ErrNone,
		GenerationId:  1,
		GroupProtocol: chosenProtocol,
		LeaderId:      memberID,
		MemberId:      memberID,
		Members: []JoinGroupMember{
			{
				MemberId:       memberID,
				MemberMetadata: chosenMetadata,
			},
		},
	}

	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleSyncGroup(ctx context.Context, header *RequestHeader, body []byte) ([]byte, error) {
	dec := NewDecoder(body)
	req, err := DecodeSyncGroupRequest(dec, header.ApiVersion)
	if err != nil {
		h.logger.WarnContext(ctx, "failed to decode sync group request", slog.Any("error", err))
		resp := &SyncGroupResponse{
			CorrelationId: header.CorrelationId,
			ErrorCode:     ErrUnknownTopicOrPartition,
		}
		return resp.Encode(header.ApiVersion), nil
	}

	// Check if an assignment was provided for this member
	var memberAssignment []byte
	for _, a := range req.GroupAssignment {
		if a.MemberId == req.MemberId && len(a.Assignment) > 0 {
			memberAssignment = a.Assignment
			break
		}
	}

	// Fallback to default assignment if leader didn't provide raw bytes
	if len(memberAssignment) == 0 {
		allTopics, _ := h.broker.ListTopics(ctx)
		topicList := make([]string, 0, len(allTopics))
		for t := range allTopics {
			topicList = append(topicList, t)
		}
		memberAssignment = BuildDefaultMemberAssignment(topicList, 1)
	}

	resp := &SyncGroupResponse{
		CorrelationId:    header.CorrelationId,
		ThrottleTime:     0,
		ErrorCode:        ErrNone,
		MemberAssignment: memberAssignment,
	}

	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleHeartbeat(header *RequestHeader, body []byte) ([]byte, error) {
	resp := &HeartbeatResponse{
		CorrelationId: header.CorrelationId,
		ThrottleTime:  0,
		ErrorCode:     ErrNone,
	}
	return resp.Encode(header.ApiVersion), nil
}

func (h *Handler) handleLeaveGroup(header *RequestHeader, body []byte) ([]byte, error) {
	resp := &LeaveGroupResponse{
		CorrelationId: header.CorrelationId,
		ThrottleTime:  0,
		ErrorCode:     ErrNone,
	}
	return resp.Encode(header.ApiVersion), nil
}
