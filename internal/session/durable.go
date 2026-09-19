package session

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Canonical message vocabulary. These string constants are the storage
// representation in agent_messages (format_version 1); the session.v2 proto
// enums are the wire representation and the transport maps between them.

// Message roles.
const (
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleTool      = "tool"
)

// MessageStatus describes the completeness of a message's own content, not the
// execution status of the request it belongs to.
const (
	MessageStatusComplete    = "complete"
	MessageStatusPartial     = "partial"
	MessageStatusInterrupted = "interrupted"
)

// ExecutionStatus is the lifecycle of the request root (the initiating user
// message). queued and running are nonterminal.
const (
	ExecutionQueued      = "queued"
	ExecutionRunning     = "running"
	ExecutionCompleted   = "completed"
	ExecutionFailed      = "failed"
	ExecutionCancelled   = "cancelled"
	ExecutionInterrupted = "interrupted"
)

// Content block types.
const (
	BlockText       = "text"
	BlockToolCall   = "tool_call"
	BlockToolResult = "tool_result"
	BlockOpaque     = "opaque"
)

// Tool result statuses.
const (
	ToolResultSuccess   = "success"
	ToolResultError     = "error"
	ToolResultCancelled = "cancelled"
)

// CanonicalFormatVersion is the block serialization this build writes.
const CanonicalFormatVersion = 1

// ContentBlock is one ordered piece of a canonical message. Its JSON encoding is
// the storage contract in agent_messages.content; field names mirror
// session.v2.ContentBlock so the stored and wire shapes stay aligned
// (arguments/schema text stays a JSON string, exactly as the proto carries it).
type ContentBlock struct {
	Type          string         `json:"type"`
	Text          string         `json:"text,omitempty"`
	ID            string         `json:"id,omitempty"`
	Name          string         `json:"name,omitempty"`
	ArgumentsJSON string         `json:"arguments_json,omitempty"`
	ToolCallID    string         `json:"tool_call_id,omitempty"`
	Status        string         `json:"status,omitempty"`
	Content       []ContentBlock `json:"content,omitempty"`
	ErrorCode     string         `json:"error_code,omitempty"`
	Provider      string         `json:"provider,omitempty"`
	OpaqueType    string         `json:"opaque_type,omitempty"`
	DataJSON      []byte         `json:"data_json,omitempty"`
}

// validate enforces the role/block combinations the canonical model allows.
func (b ContentBlock) validate(index int) error {
	switch b.Type {
	case BlockText:
		return nil
	case BlockToolCall:
		if b.ID == "" || b.Name == "" {
			return fmt.Errorf("content block %d: tool_call needs id and name", index)
		}
		if b.ArgumentsJSON != "" && !json.Valid([]byte(b.ArgumentsJSON)) {
			return fmt.Errorf("content block %d: tool_call arguments_json is not valid JSON", index)
		}
		return nil
	case BlockToolResult:
		if b.ToolCallID == "" {
			return fmt.Errorf("content block %d: tool_result needs tool_call_id", index)
		}
		switch b.Status {
		case ToolResultSuccess, ToolResultError, ToolResultCancelled:
		default:
			return fmt.Errorf("content block %d: tool_result status %q is invalid", index, b.Status)
		}
		for i, nested := range b.Content {
			if err := nested.validate(i); err != nil {
				return err
			}
		}
		return nil
	case BlockOpaque:
		if b.Provider == "" || b.OpaqueType == "" {
			return fmt.Errorf("content block %d: opaque needs provider and type", index)
		}
		return nil
	default:
		return fmt.Errorf("content block %d: unknown type %q", index, b.Type)
	}
}

// ValidateContent checks a whole ordered block list.
func ValidateContent(blocks []ContentBlock) error {
	if len(blocks) == 0 {
		return fmt.Errorf("content must not be empty")
	}
	for i, b := range blocks {
		if err := b.validate(i); err != nil {
			return err
		}
	}
	return nil
}

// toolCallIDs returns every tool-call id in the list, recursively.
func toolCallIDs(blocks []ContentBlock, out *[]string) {
	for _, b := range blocks {
		switch b.Type {
		case BlockToolCall:
			*out = append(*out, b.ID)
		case BlockToolResult:
			toolCallIDs(b.Content, out)
		}
	}
}

// resultRefs returns every tool-call id a result block refers to.
func resultRefs(blocks []ContentBlock, out *[]string) {
	for _, b := range blocks {
		switch b.Type {
		case BlockToolResult:
			*out = append(*out, b.ToolCallID)
			resultRefs(b.Content, out)
		}
	}
}

// ToolDefinition is one tool the session's frozen configuration exposes.
type ToolDefinition struct {
	Name            string `json:"name"`
	Description     string `json:"description,omitempty"`
	InputSchemaJSON string `json:"input_schema_json,omitempty"`
	Version         string `json:"version,omitempty"`
}

// SessionConfig is the immutable configuration resolved and frozen at session
// creation. CredentialRef is a reference into private credential storage, never
// a credential value.
type SessionConfig struct {
	SessionID           string
	FormatVersion       int32
	ConfigHash          string
	SystemPrompt        string
	Provider            string
	Model               string
	BaseURL             string
	CredentialRef       string
	ContextWindowTokens int64
	OutputLimitTokens   int64
	Tools               []ToolDefinition
	CreatedAt           string
}

// Validate checks the invariants the store also enforces, so a bad configuration
// fails before a round trip.
func (c SessionConfig) Validate() error {
	if c.FormatVersion <= 0 {
		return fmt.Errorf("config format_version must be positive")
	}
	if c.ConfigHash == "" {
		return fmt.Errorf("config_hash is required")
	}
	if c.Model == "" || c.BaseURL == "" {
		return fmt.Errorf("config model and base_url are required")
	}
	if c.ContextWindowTokens <= 0 || c.OutputLimitTokens <= 0 {
		return fmt.Errorf("config context_window_tokens and output_limit_tokens must be positive")
	}
	if c.OutputLimitTokens >= c.ContextWindowTokens {
		return fmt.Errorf("config output_limit_tokens must be smaller than context_window_tokens")
	}
	return nil
}

// Checkpoint is one compaction result: an immutable summary of the complete
// prefix through CoveredThroughSeq.
type Checkpoint struct {
	ID                     string
	SessionID              string
	ParentCheckpointID     string
	CoveredThroughSeq      int64
	SourceHeadSeq          int64
	SourceRevision         int64
	ConfigHash             string
	ActiveRequestMessageID string
	ResumeAfterSeq         int64
	Summary                []ContentBlock
	FormatVersion          int32
	PromptVersion          string
	SummarizerProvider     string
	SummarizerModel        string
	EstimatedTokensBefore  int64
	EstimatedTokensAfter   int64
	GenerationUsageJSON    string
	CreatedAt              string
}

// Lease is a renewable, fenced grant of execution ownership.
type Lease struct {
	Owner      string
	Generation int64
	ExpiresAt  string
}

// MutationGuard carries the idempotency key and fencing preconditions for one
// durable mutation. ExpectedRevision <= 0 means "do not check".
type MutationGuard struct {
	MutationID       string
	MutationHash     string
	ExpectedRevision int64
	LeaseGeneration  int64
}

// MessageDraft is a message a runtime wants to persist. The store assigns seq.
type MessageDraft struct {
	ID                   string
	Role                 string
	Content              []ContentBlock
	FormatVersion        int32
	Status               string
	ReplyToMessageID     string
	UsageJSON            string
	ProviderMetadataJSON string
}

// Validate checks a single draft before it is sent to the store.
func (d MessageDraft) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("message id is required")
	}
	switch d.Role {
	case RoleAssistant, RoleTool:
	default:
		return fmt.Errorf("message role %q is not appendable", d.Role)
	}
	if d.FormatVersion <= 0 {
		return fmt.Errorf("message format_version must be positive")
	}
	switch d.Status {
	case MessageStatusComplete, MessageStatusPartial, MessageStatusInterrupted:
	default:
		return fmt.Errorf("message_status %q is invalid", d.Status)
	}
	return ValidateContent(d.Content)
}

// ---------------------------------------------------------------- port params

// BeginRequestParams admits a new request for an existing session.
type BeginRequestParams struct {
	SessionID        string
	UserID           string
	RequestMessageID string
	ClientRequestID  string
	RequestHash      string
	Content          []ContentBlock
	FormatVersion    int32
}

// BeginRequestResult is the admission outcome. Deduplicated is true when the
// client request id and hash matched an existing root.
type BeginRequestResult struct {
	Session        Session
	RequestMessage Message
	Deduplicated   bool
}

// LeaseParams requests fenced ownership of a request.
type LeaseParams struct {
	SessionID        string
	RequestMessageID string
	Owner            string
	TTLSeconds       int32
}

// LeaseResult carries the refreshed session and the granted lease.
type LeaseResult struct {
	Session Session
	Lease   Lease
}

// RenewLeaseParams extends a held lease.
type RenewLeaseParams struct {
	SessionID  string
	Owner      string
	Generation int64
	TTLSeconds int32
}

// ReleaseLeaseParams releases a held lease.
type ReleaseLeaseParams struct {
	SessionID  string
	Owner      string
	Generation int64
}

// AppendParams commits one idempotent message batch.
type AppendParams struct {
	SessionID        string
	RequestMessageID string
	Guard            MutationGuard
	Messages         []MessageDraft
}

// AppendResult is the committed batch.
type AppendResult struct {
	Session      Session
	Messages     []Message
	Deduplicated bool
}

// FinishParams writes the final batch and the terminal root state.
type FinishParams struct {
	SessionID        string
	RequestMessageID string
	Guard            MutationGuard
	Status           string
	Messages         []MessageDraft
	ErrorCode        string
}

// FinishResult is the terminal outcome.
type FinishResult struct {
	Session        Session
	RequestMessage Message
	Messages       []Message
	Deduplicated   bool
}

// CancelRequestParams asks to cancel a request. RequestMessageID empty means the
// session's active request.
type CancelRequestParams struct {
	SessionID        string
	UserID           string
	RequestMessageID string
}

// CancelResult reports the request root after cancellation.
type CancelResult struct {
	RequestMessage Message
	Deduplicated   bool
}

// PublishParams activates a compaction checkpoint.
type PublishParams struct {
	SessionID  string
	Guard      MutationGuard
	Checkpoint Checkpoint
}

// PublishResult is the activated checkpoint.
type PublishResult struct {
	Session      Session
	Checkpoint   Checkpoint
	Deduplicated bool
}

// ContextState is the durable input the runtime hydrates a request from.
type ContextState struct {
	Session       Session
	Config        SessionConfig
	Checkpoint    Checkpoint
	HasCheckpoint bool
}

// ValidateContentJSON validates the content array of a raw stored message.
func ValidateContentJSON(raw string) error {
	var blocks []ContentBlock
	if err := json.Unmarshal([]byte(raw), &blocks); err != nil {
		return fmt.Errorf("content is not a JSON array of blocks: %w", err)
	}
	return ValidateContent(blocks)
}

// ContentBlocks decodes a stored content payload.
func ContentBlocks(raw string) ([]ContentBlock, error) {
	if raw == "" {
		return nil, nil
	}
	var blocks []ContentBlock
	if err := json.Unmarshal([]byte(raw), &blocks); err != nil {
		return nil, fmt.Errorf("decode content: %w", err)
	}
	return blocks, nil
}

// EncodeContentBlocks serializes blocks for storage.
func EncodeContentBlocks(blocks []ContentBlock) (string, error) {
	if blocks == nil {
		blocks = []ContentBlock{}
	}
	b, err := json.Marshal(blocks)
	if err != nil {
		return "", fmt.Errorf("encode content: %w", err)
	}
	return string(b), nil
}

// DuplicateToolCallIDs reports tool-call ids that appear more than once in the
// given messages, which would make result links ambiguous.
func DuplicateToolCallIDs(messages []Message) []string {
	seen := map[string]int{}
	for _, m := range messages {
		blocks, err := ContentBlocks(m.ContentJSON)
		if err != nil {
			continue
		}
		var ids []string
		toolCallIDs(blocks, &ids)
		for _, id := range ids {
			seen[id]++
		}
	}
	var dupes []string
	for id, n := range seen {
		if n > 1 {
			dupes = append(dupes, id)
		}
	}
	sort.Strings(dupes)
	return dupes
}
