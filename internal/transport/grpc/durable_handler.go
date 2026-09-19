package grpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/session-manager/internal/session"
)

// DurableHandler serves session.v2.SessionService on top of the durable domain
// slice. Handlers are thin: decode the request, extract credentials from
// metadata, call the slice service, and map errors via toStatus. Both versions
// stay registered while the platform migrates.
type DurableHandler struct {
	v2.UnimplementedSessionServiceServer

	svc *session.DurableService
}

// NewDurableHandler wires the v2 gRPC handlers to svc.
func NewDurableHandler(svc *session.DurableService) *DurableHandler {
	return &DurableHandler{svc: svc}
}

func (h *DurableHandler) CreateSession(ctx context.Context, req *v2.CreateSessionRequest) (*v2.CreateSessionResponse, error) {
	cfg, err := configFromProto(req.GetConfig())
	if err != nil {
		return nil, toStatus(invalidArgument(err))
	}
	sess, _, err := h.svc.CreateSession(ctx, req.GetUserId(), req.GetAgentId(), cfg)
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.CreateSessionResponse{Session: sessionToProto(sess)}, nil
}

func (h *DurableHandler) GetSession(ctx context.Context, req *v2.GetSessionRequest) (*v2.GetSessionResponse, error) {
	sess, err := h.svc.GetSession(ctx, req.GetSessionId(), req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.GetSessionResponse{Session: sessionToProto(sess)}, nil
}

func (h *DurableHandler) ListSessions(ctx context.Context, req *v2.ListSessionsRequest) (*v2.ListSessionsResponse, error) {
	sessions, hasMore, err := h.svc.ListSessions(ctx, req.GetUserId(), req.GetLimit(), req.GetOffset())
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &v2.ListSessionsResponse{HasMore: hasMore}
	for _, sess := range sessions {
		resp.Sessions = append(resp.Sessions, sessionToProto(sess))
	}
	return resp, nil
}

func (h *DurableHandler) EndSession(ctx context.Context, req *v2.EndSessionRequest) (*v2.EndSessionResponse, error) {
	sess, err := h.svc.EndSession(ctx, req.GetSessionId(), req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.EndSessionResponse{Session: sessionToProto(sess)}, nil
}

func (h *DurableHandler) DeleteSession(ctx context.Context, req *v2.DeleteSessionRequest) (*v2.DeleteSessionResponse, error) {
	if err := h.svc.DeleteSession(ctx, req.GetSessionId(), req.GetUserId()); err != nil {
		return nil, toStatus(err)
	}
	return &v2.DeleteSessionResponse{}, nil
}

func (h *DurableHandler) SetTitle(ctx context.Context, req *v2.SetTitleRequest) (*v2.SetTitleResponse, error) {
	sess, err := h.svc.SetTitle(ctx, req.GetSessionId(), req.GetTitle(), req.GetUserId(), presentedTokens(ctx))
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.SetTitleResponse{Session: sessionToProto(sess)}, nil
}

func (h *DurableHandler) BeginRequest(ctx context.Context, req *v2.BeginRequestRequest) (*v2.BeginRequestResponse, error) {
	content, err := blocksFromProto(req.GetContent())
	if err != nil {
		return nil, toStatus(invalidArgument(err))
	}
	res, err := h.svc.BeginRequest(ctx, presentedTokens(ctx), session.BeginRequestParams{
		SessionID:        req.GetSessionId(),
		UserID:           req.GetUserId(),
		RequestMessageID: newMessageID(),
		ClientRequestID:  req.GetClientRequestId(),
		RequestHash:      req.GetRequestHash(),
		Content:          content,
		FormatVersion:    int32(req.GetFormatVersion()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.BeginRequestResponse{
		Session:        sessionToProto(res.Session),
		RequestMessage: messageToProto(res.RequestMessage),
		Deduplicated:   res.Deduplicated,
	}, nil
}

func (h *DurableHandler) AcquireLease(ctx context.Context, req *v2.AcquireLeaseRequest) (*v2.AcquireLeaseResponse, error) {
	res, err := h.svc.AcquireLease(ctx, presentedTokens(ctx), session.LeaseParams{
		SessionID:        req.GetSessionId(),
		RequestMessageID: req.GetRequestMessageId(),
		Owner:            req.GetOwner(),
		TTLSeconds:       req.GetTtlSeconds(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.AcquireLeaseResponse{Session: sessionToProto(res.Session), Lease: leaseToProto(res.Lease)}, nil
}

func (h *DurableHandler) RenewLease(ctx context.Context, req *v2.RenewLeaseRequest) (*v2.RenewLeaseResponse, error) {
	lease, err := h.svc.RenewLease(ctx, presentedTokens(ctx), session.RenewLeaseParams{
		SessionID:  req.GetSessionId(),
		Owner:      req.GetOwner(),
		Generation: req.GetGeneration(),
		TTLSeconds: req.GetTtlSeconds(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.RenewLeaseResponse{Lease: leaseToProto(lease)}, nil
}

func (h *DurableHandler) ReleaseLease(ctx context.Context, req *v2.ReleaseLeaseRequest) (*v2.ReleaseLeaseResponse, error) {
	if _, err := h.svc.ReleaseLease(ctx, presentedTokens(ctx), session.ReleaseLeaseParams{
		SessionID:  req.GetSessionId(),
		Owner:      req.GetOwner(),
		Generation: req.GetGeneration(),
	}); err != nil {
		return nil, toStatus(err)
	}
	return &v2.ReleaseLeaseResponse{}, nil
}

func (h *DurableHandler) AppendMessages(ctx context.Context, req *v2.AppendMessagesRequest) (*v2.AppendMessagesResponse, error) {
	drafts, err := draftsFromProto(req.GetMessages())
	if err != nil {
		return nil, toStatus(invalidArgument(err))
	}
	res, err := h.svc.AppendMessages(ctx, presentedTokens(ctx), session.AppendParams{
		SessionID:        req.GetSessionId(),
		RequestMessageID: req.GetRequestMessageId(),
		Guard:            guardFromProto(req.GetGuard()),
		Messages:         drafts,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.AppendMessagesResponse{
		Session:      sessionToProto(res.Session),
		Messages:     messagesToProto(res.Messages),
		Deduplicated: res.Deduplicated,
	}, nil
}

func (h *DurableHandler) FinishRequest(ctx context.Context, req *v2.FinishRequestRequest) (*v2.FinishRequestResponse, error) {
	drafts, err := draftsFromProto(req.GetMessages())
	if err != nil {
		return nil, toStatus(invalidArgument(err))
	}
	res, err := h.svc.FinishRequest(ctx, presentedTokens(ctx), session.FinishParams{
		SessionID:        req.GetSessionId(),
		RequestMessageID: req.GetRequestMessageId(),
		Guard:            guardFromProto(req.GetGuard()),
		Status:           executionStatusFromProto(req.GetStatus()),
		Messages:         drafts,
		ErrorCode:        req.GetErrorCode(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.FinishRequestResponse{
		Session:        sessionToProto(res.Session),
		RequestMessage: messageToProto(res.RequestMessage),
		Messages:       messagesToProto(res.Messages),
		Deduplicated:   res.Deduplicated,
	}, nil
}

func (h *DurableHandler) CancelRequest(ctx context.Context, req *v2.CancelRequestRequest) (*v2.CancelRequestResponse, error) {
	res, err := h.svc.CancelRequest(ctx, req.GetSessionId(), req.GetUserId(), presentedTokens(ctx), req.GetRequestMessageId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.CancelRequestResponse{RequestMessage: messageToProto(res.RequestMessage)}, nil
}

func (h *DurableHandler) GetMessages(ctx context.Context, req *v2.GetMessagesRequest) (*v2.GetMessagesResponse, error) {
	msgs, hasMore, revision, err := h.svc.GetMessages(ctx, req.GetSessionId(), req.GetUserId(), presentedTokens(ctx), req.GetBeforeSeq(), req.GetLimit())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.GetMessagesResponse{
		Messages: messagesToProto(msgs),
		HasMore:  hasMore,
		Revision: revision,
	}, nil
}

func (h *DurableHandler) GetExecutionContext(ctx context.Context, req *v2.GetExecutionContextRequest) (*v2.GetExecutionContextResponse, error) {
	state, err := h.svc.GetExecutionContext(ctx, req.GetSessionId(), presentedTokens(ctx))
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &v2.GetExecutionContextResponse{
		Session: sessionToProto(state.Session),
		Config:  configToProto(state.Config),
	}
	if state.HasCheckpoint {
		resp.Checkpoint = checkpointToProto(state.Checkpoint)
	}
	return resp, nil
}

func (h *DurableHandler) PublishCheckpoint(ctx context.Context, req *v2.PublishCheckpointRequest) (*v2.PublishCheckpointResponse, error) {
	cp, err := checkpointFromProto(req.GetCheckpoint())
	if err != nil {
		return nil, toStatus(invalidArgument(err))
	}
	res, err := h.svc.PublishCheckpoint(ctx, presentedTokens(ctx), session.PublishParams{
		SessionID:  req.GetSessionId(),
		Guard:      guardFromProto(req.GetGuard()),
		Checkpoint: cp,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.PublishCheckpointResponse{
		Session:      sessionToProto(res.Session),
		Checkpoint:   checkpointToProto(res.Checkpoint),
		Deduplicated: res.Deduplicated,
	}, nil
}

// ------------------------------------------------------------- conversions

func invalidArgument(err error) error {
	return &session.Error{Kind: session.KindInvalidArgument, Message: err.Error()}
}

// newMessageID returns the canonical id for an admitted request root. The
// session-manager owns canonical ids; the caller's client_request_id stays the
// deduplication key.
func newMessageID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice; fall back to a timestamp-based
		// id rather than failing admission.
		return fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	return "msg-" + hex.EncodeToString(b[:])
}

func sessionToProto(s session.Session) *v2.Session {
	out := &v2.Session{
		Id:                     s.ID,
		UserId:                 s.UserID,
		Status:                 sessionStatusToProto(s.Status),
		AgentId:                s.AgentID,
		Title:                  s.Title,
		LastSeq:                s.LastSeq,
		Revision:               s.Revision,
		ActiveCheckpointId:     s.ActiveCheckpointID,
		ActiveRequestMessageId: s.ActiveRequestMessageID,
		CreatedAt:              timestampToProto(s.CreatedAt),
		LastActive:             timestampToProto(s.LastActive),
		EndedAt:                timestampToProto(s.EndedAt),
	}
	return out
}

func configToProto(c session.SessionConfig) *v2.SessionConfig {
	out := &v2.SessionConfig{
		SessionId:           c.SessionID,
		FormatVersion:       uint32(c.FormatVersion),
		ConfigHash:          c.ConfigHash,
		SystemPrompt:        c.SystemPrompt,
		Provider:            c.Provider,
		Model:               c.Model,
		BaseUrl:             c.BaseURL,
		CredentialRef:       c.CredentialRef,
		ContextWindowTokens: c.ContextWindowTokens,
		OutputLimitTokens:   c.OutputLimitTokens,
		CreatedAt:           timestampToProto(c.CreatedAt),
	}
	for _, t := range c.Tools {
		out.Tools = append(out.Tools, &v2.ToolDefinition{
			Name:            t.Name,
			Description:     t.Description,
			InputSchemaJson: t.InputSchemaJSON,
			Version:         t.Version,
		})
	}
	return out
}

func configFromProto(p *v2.SessionConfig) (session.SessionConfig, error) {
	if p == nil {
		return session.SessionConfig{}, fmt.Errorf("config is required")
	}
	cfg := session.SessionConfig{
		FormatVersion:       int32(p.GetFormatVersion()),
		ConfigHash:          p.GetConfigHash(),
		SystemPrompt:        p.GetSystemPrompt(),
		Provider:            p.GetProvider(),
		Model:               p.GetModel(),
		BaseURL:             p.GetBaseUrl(),
		CredentialRef:       p.GetCredentialRef(),
		ContextWindowTokens: p.GetContextWindowTokens(),
		OutputLimitTokens:   p.GetOutputLimitTokens(),
	}
	for _, t := range p.GetTools() {
		cfg.Tools = append(cfg.Tools, session.ToolDefinition{
			Name:            t.GetName(),
			Description:     t.GetDescription(),
			InputSchemaJSON: t.GetInputSchemaJson(),
			Version:         t.GetVersion(),
		})
	}
	return cfg, nil
}

func messageToProto(m session.Message) *v2.Message {
	blocks, _ := session.ContentBlocks(m.ContentJSON)
	out := &v2.Message{
		Id:               m.ID,
		SessionId:        m.RequestMessageID,
		Seq:              int64(m.Seq),
		RequestMessageId: m.RequestMessageID,
		Role:             roleToProto(m.Role),
		Content:          blocksToProto(blocks),
		FormatVersion:    uint32(m.FormatVersion),
		Status:           messageStatusToProto(m.Status),
		ReplyToMessageId: m.ReplyToMessageID,
		Usage:            usageToProto(m.UsageJSON),
		ProviderMetadata: metadataToProto(m.ProviderMetadataJSON),
		CreatedAt:        timestampToProto(m.CreatedAt),
	}
	if m.ClientRequestID != "" || m.ExecutionStatus != "" {
		out.Request = &v2.RequestExecution{
			ClientRequestId:       m.ClientRequestID,
			RequestHash:           m.RequestHash,
			Status:                executionStatusToProto(m.ExecutionStatus),
			StartedAt:             timestampToProto(m.StartedAt),
			FinishedAt:            timestampToProto(m.FinishedAt),
			ErrorCode:             m.ErrorCode,
			CancellationRequested: m.CancellationRequested,
		}
	}
	return out
}

func messagesToProto(msgs []session.Message) []*v2.Message {
	out := make([]*v2.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, messageToProto(m))
	}
	return out
}

func blocksToProto(blocks []session.ContentBlock) []*v2.ContentBlock {
	out := make([]*v2.ContentBlock, 0, len(blocks))
	for _, b := range blocks {
		out = append(out, blockToProto(b))
	}
	return out
}

func blockToProto(b session.ContentBlock) *v2.ContentBlock {
	switch b.Type {
	case session.BlockText:
		return &v2.ContentBlock{Value: &v2.ContentBlock_Text{Text: &v2.TextBlock{Text: b.Text}}}
	case session.BlockToolCall:
		return &v2.ContentBlock{Value: &v2.ContentBlock_ToolCall{ToolCall: &v2.ToolCallBlock{
			Id: b.ID, Name: b.Name, ArgumentsJson: b.ArgumentsJSON,
		}}}
	case session.BlockToolResult:
		return &v2.ContentBlock{Value: &v2.ContentBlock_ToolResult{ToolResult: &v2.ToolResultBlock{
			ToolCallId: b.ToolCallID,
			Status:     toolResultStatusToProto(b.Status),
			Content:    blocksToProto(b.Content),
			ErrorCode:  b.ErrorCode,
		}}}
	case session.BlockOpaque:
		return &v2.ContentBlock{Value: &v2.ContentBlock_Opaque{Opaque: &v2.OpaqueBlock{
			Provider: b.Provider, Type: b.OpaqueType, DataJson: b.DataJSON,
		}}}
	default:
		return &v2.ContentBlock{}
	}
}

func blocksFromProto(blocks []*v2.ContentBlock) ([]session.ContentBlock, error) {
	out := make([]session.ContentBlock, 0, len(blocks))
	for _, b := range blocks {
		cb, err := blockFromProto(b)
		if err != nil {
			return nil, err
		}
		out = append(out, cb)
	}
	return out, nil
}

func blockFromProto(b *v2.ContentBlock) (session.ContentBlock, error) {
	switch v := b.GetValue().(type) {
	case *v2.ContentBlock_Text:
		return session.ContentBlock{Type: session.BlockText, Text: v.Text.GetText()}, nil
	case *v2.ContentBlock_ToolCall:
		return session.ContentBlock{
			Type: session.BlockToolCall, ID: v.ToolCall.GetId(),
			Name: v.ToolCall.GetName(), ArgumentsJSON: v.ToolCall.GetArgumentsJson(),
		}, nil
	case *v2.ContentBlock_ToolResult:
		nested, err := blocksFromProto(v.ToolResult.GetContent())
		if err != nil {
			return session.ContentBlock{}, err
		}
		return session.ContentBlock{
			Type: session.BlockToolResult, ToolCallID: v.ToolResult.GetToolCallId(),
			Status:  toolResultStatusFromProto(v.ToolResult.GetStatus()),
			Content: nested, ErrorCode: v.ToolResult.GetErrorCode(),
		}, nil
	case *v2.ContentBlock_Opaque:
		return session.ContentBlock{
			Type: session.BlockOpaque, Provider: v.Opaque.GetProvider(),
			OpaqueType: v.Opaque.GetType(), DataJSON: v.Opaque.GetDataJson(),
		}, nil
	default:
		return session.ContentBlock{}, fmt.Errorf("content block has no value")
	}
}

func draftsFromProto(msgs []*v2.MessageDraft) ([]session.MessageDraft, error) {
	out := make([]session.MessageDraft, 0, len(msgs))
	for _, m := range msgs {
		blocks, err := blocksFromProto(m.GetContent())
		if err != nil {
			return nil, err
		}
		out = append(out, session.MessageDraft{
			ID:                   m.GetId(),
			Role:                 roleFromProto(m.GetRole()),
			Content:              blocks,
			FormatVersion:        int32(m.GetFormatVersion()),
			Status:               messageStatusFromProto(m.GetStatus()),
			ReplyToMessageID:     m.GetReplyToMessageId(),
			UsageJSON:            usageFromProto(m.GetUsage()),
			ProviderMetadataJSON: metadataFromProto(m.GetProviderMetadata()),
		})
	}
	return out, nil
}

func guardFromProto(g *v2.MutationGuard) session.MutationGuard {
	if g == nil {
		return session.MutationGuard{}
	}
	return session.MutationGuard{
		MutationID:       g.GetMutationId(),
		MutationHash:     g.GetMutationHash(),
		ExpectedRevision: g.GetExpectedRevision(),
		LeaseGeneration:  g.GetLeaseGeneration(),
	}
}

func checkpointToProto(cp session.Checkpoint) *v2.ContextCheckpoint {
	return &v2.ContextCheckpoint{
		Id:                     cp.ID,
		SessionId:              cp.SessionID,
		ParentCheckpointId:     cp.ParentCheckpointID,
		CoveredThroughSeq:      cp.CoveredThroughSeq,
		SourceHeadSeq:          cp.SourceHeadSeq,
		SourceRevision:         cp.SourceRevision,
		ConfigHash:             cp.ConfigHash,
		ActiveRequestMessageId: cp.ActiveRequestMessageID,
		ResumeAfterSeq:         cp.ResumeAfterSeq,
		Summary:                blocksToProto(cp.Summary),
		FormatVersion:          uint32(cp.FormatVersion),
		PromptVersion:          cp.PromptVersion,
		SummarizerProvider:     cp.SummarizerProvider,
		SummarizerModel:        cp.SummarizerModel,
		EstimatedTokensBefore:  cp.EstimatedTokensBefore,
		EstimatedTokensAfter:   cp.EstimatedTokensAfter,
		GenerationUsage:        usageToProto(cp.GenerationUsageJSON),
		CreatedAt:              timestampToProto(cp.CreatedAt),
	}
}

func checkpointFromProto(cp *v2.ContextCheckpoint) (session.Checkpoint, error) {
	if cp == nil {
		return session.Checkpoint{}, fmt.Errorf("checkpoint is required")
	}
	summary, err := blocksFromProto(cp.GetSummary())
	if err != nil {
		return session.Checkpoint{}, err
	}
	return session.Checkpoint{
		ID:                     cp.GetId(),
		ParentCheckpointID:     cp.GetParentCheckpointId(),
		CoveredThroughSeq:      cp.GetCoveredThroughSeq(),
		SourceHeadSeq:          cp.GetSourceHeadSeq(),
		SourceRevision:         cp.GetSourceRevision(),
		ConfigHash:             cp.GetConfigHash(),
		ActiveRequestMessageID: cp.GetActiveRequestMessageId(),
		ResumeAfterSeq:         cp.GetResumeAfterSeq(),
		Summary:                summary,
		FormatVersion:          int32(cp.GetFormatVersion()),
		PromptVersion:          cp.GetPromptVersion(),
		SummarizerProvider:     cp.GetSummarizerProvider(),
		SummarizerModel:        cp.GetSummarizerModel(),
		EstimatedTokensBefore:  cp.GetEstimatedTokensBefore(),
		EstimatedTokensAfter:   cp.GetEstimatedTokensAfter(),
		GenerationUsageJSON:    usageFromProto(cp.GetGenerationUsage()),
	}, nil
}

func leaseToProto(l session.Lease) *v2.Lease {
	return &v2.Lease{Owner: l.Owner, Generation: l.Generation, ExpiresAt: timestampToProto(l.ExpiresAt)}
}

func timestampToProto(s string) *timestamppb.Timestamp {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	return timestamppb.New(t)
}

func usageToProto(raw string) *v2.TokenUsage {
	if raw == "" {
		return nil
	}
	var u struct {
		InputTokens       int64 `json:"input_tokens"`
		CachedInputTokens int64 `json:"cached_input_tokens"`
		OutputTokens      int64 `json:"output_tokens"`
		ReasoningTokens   int64 `json:"reasoning_tokens"`
		TotalTokens       int64 `json:"total_tokens"`
	}
	if err := json.Unmarshal([]byte(raw), &u); err != nil {
		return nil
	}
	return &v2.TokenUsage{
		InputTokens:       u.InputTokens,
		CachedInputTokens: u.CachedInputTokens,
		OutputTokens:      u.OutputTokens,
		ReasoningTokens:   u.ReasoningTokens,
		TotalTokens:       u.TotalTokens,
	}
}

func usageFromProto(u *v2.TokenUsage) string {
	if u == nil {
		return ""
	}
	b, err := json.Marshal(map[string]int64{
		"input_tokens":        u.GetInputTokens(),
		"cached_input_tokens": u.GetCachedInputTokens(),
		"output_tokens":       u.GetOutputTokens(),
		"reasoning_tokens":    u.GetReasoningTokens(),
		"total_tokens":        u.GetTotalTokens(),
	})
	if err != nil {
		return ""
	}
	return string(b)
}

func metadataToProto(raw string) *structpb.Struct {
	if raw == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return s
}

func metadataFromProto(s *structpb.Struct) string {
	if s == nil {
		return ""
	}
	b, err := json.Marshal(s.AsMap())
	if err != nil {
		return ""
	}
	return string(b)
}

func sessionStatusToProto(status string) v2.SessionStatus {
	switch status {
	case "active":
		return v2.SessionStatus_SESSION_STATUS_ACTIVE
	case "ended":
		return v2.SessionStatus_SESSION_STATUS_ENDED
	default:
		return v2.SessionStatus_SESSION_STATUS_UNSPECIFIED
	}
}

func roleToProto(role string) v2.MessageRole {
	switch role {
	case session.RoleUser:
		return v2.MessageRole_MESSAGE_ROLE_USER
	case session.RoleAssistant:
		return v2.MessageRole_MESSAGE_ROLE_ASSISTANT
	case session.RoleTool:
		return v2.MessageRole_MESSAGE_ROLE_TOOL
	default:
		// Legacy v1 turns (system/config) have no v2 role.
		return v2.MessageRole_MESSAGE_ROLE_UNSPECIFIED
	}
}

func roleFromProto(role v2.MessageRole) string {
	switch role {
	case v2.MessageRole_MESSAGE_ROLE_USER:
		return session.RoleUser
	case v2.MessageRole_MESSAGE_ROLE_ASSISTANT:
		return session.RoleAssistant
	case v2.MessageRole_MESSAGE_ROLE_TOOL:
		return session.RoleTool
	default:
		return ""
	}
}

func messageStatusToProto(status string) v2.MessageStatus {
	switch status {
	case session.MessageStatusComplete:
		return v2.MessageStatus_MESSAGE_STATUS_COMPLETE
	case session.MessageStatusPartial:
		return v2.MessageStatus_MESSAGE_STATUS_PARTIAL
	case session.MessageStatusInterrupted:
		return v2.MessageStatus_MESSAGE_STATUS_INTERRUPTED
	default:
		return v2.MessageStatus_MESSAGE_STATUS_UNSPECIFIED
	}
}

func messageStatusFromProto(status v2.MessageStatus) string {
	switch status {
	case v2.MessageStatus_MESSAGE_STATUS_COMPLETE:
		return session.MessageStatusComplete
	case v2.MessageStatus_MESSAGE_STATUS_PARTIAL:
		return session.MessageStatusPartial
	case v2.MessageStatus_MESSAGE_STATUS_INTERRUPTED:
		return session.MessageStatusInterrupted
	default:
		return session.MessageStatusComplete
	}
}

func executionStatusToProto(status string) v2.ExecutionStatus {
	switch status {
	case session.ExecutionQueued:
		return v2.ExecutionStatus_EXECUTION_STATUS_QUEUED
	case session.ExecutionRunning:
		return v2.ExecutionStatus_EXECUTION_STATUS_RUNNING
	case session.ExecutionCompleted:
		return v2.ExecutionStatus_EXECUTION_STATUS_COMPLETED
	case session.ExecutionFailed:
		return v2.ExecutionStatus_EXECUTION_STATUS_FAILED
	case session.ExecutionCancelled:
		return v2.ExecutionStatus_EXECUTION_STATUS_CANCELLED
	case session.ExecutionInterrupted:
		return v2.ExecutionStatus_EXECUTION_STATUS_INTERRUPTED
	default:
		return v2.ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED
	}
}

func executionStatusFromProto(status v2.ExecutionStatus) string {
	switch status {
	case v2.ExecutionStatus_EXECUTION_STATUS_QUEUED:
		return session.ExecutionQueued
	case v2.ExecutionStatus_EXECUTION_STATUS_RUNNING:
		return session.ExecutionRunning
	case v2.ExecutionStatus_EXECUTION_STATUS_COMPLETED:
		return session.ExecutionCompleted
	case v2.ExecutionStatus_EXECUTION_STATUS_FAILED:
		return session.ExecutionFailed
	case v2.ExecutionStatus_EXECUTION_STATUS_CANCELLED:
		return session.ExecutionCancelled
	case v2.ExecutionStatus_EXECUTION_STATUS_INTERRUPTED:
		return session.ExecutionInterrupted
	default:
		return ""
	}
}

func toolResultStatusToProto(status string) v2.ToolResultStatus {
	switch status {
	case session.ToolResultSuccess:
		return v2.ToolResultStatus_TOOL_RESULT_STATUS_SUCCESS
	case session.ToolResultError:
		return v2.ToolResultStatus_TOOL_RESULT_STATUS_ERROR
	case session.ToolResultCancelled:
		return v2.ToolResultStatus_TOOL_RESULT_STATUS_CANCELLED
	default:
		return v2.ToolResultStatus_TOOL_RESULT_STATUS_UNSPECIFIED
	}
}

func toolResultStatusFromProto(status v2.ToolResultStatus) string {
	switch status {
	case v2.ToolResultStatus_TOOL_RESULT_STATUS_SUCCESS:
		return session.ToolResultSuccess
	case v2.ToolResultStatus_TOOL_RESULT_STATUS_ERROR:
		return session.ToolResultError
	case v2.ToolResultStatus_TOOL_RESULT_STATUS_CANCELLED:
		return session.ToolResultCancelled
	default:
		return session.ToolResultSuccess
	}
}
