package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestJSONRoundTrip(t *testing.T) {
	corrID := "test-corr-id"

	t.Run("URLCreatedEvent", func(t *testing.T) {
		exp := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Millisecond)
		original := URLCreatedEvent{
			BaseEvent:   NewBaseEvent(EventTypeURLCreated, corrID),
			ShortCode:   "abcd123",
			OriginalURL: "https://example.com",
			UserID:      "user-1",
			UserEmail:   "user@example.com",
			ExpiresAt:   &exp,
		}

		b, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Failed to marshal: %v", err)
		}

		var decoded URLCreatedEvent
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatalf("Failed to unmarshal: %v", err)
		}

		// Because JSON marshaling of time truncates precision differently based on environment,
		// we use Equal logic instead of deep equal on time fields if needed.
		if original.EventID != decoded.EventID {
			t.Errorf("Expected EventID %v, got %v", original.EventID, decoded.EventID)
		}
		if original.ShortCode != decoded.ShortCode {
			t.Errorf("Expected ShortCode %v, got %v", original.ShortCode, decoded.ShortCode)
		}
	})

	t.Run("URLClickedEvent", func(t *testing.T) {
		original := URLClickedEvent{
			BaseEvent: NewBaseEvent(EventTypeURLClicked, corrID),
			ShortCode: "abcd123",
			IPHash:    "hashed-ip",
			UserAgent: "Mozilla/5.0",
			Referer:   "https://google.com",
			ClickedAt: time.Now().UTC().Truncate(time.Millisecond),
		}

		b, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Failed to marshal: %v", err)
		}

		var decoded URLClickedEvent
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatalf("Failed to unmarshal: %v", err)
		}

		if original.IPHash != decoded.IPHash {
			t.Errorf("Expected IPHash %v, got %v", original.IPHash, decoded.IPHash)
		}
	})

	t.Run("URLDeletedEvent", func(t *testing.T) {
		original := URLDeletedEvent{
			BaseEvent: NewBaseEvent(EventTypeURLDeleted, corrID),
			ShortCode: "abcd123",
			UserID:    "user-1",
			UserEmail: "user@example.com",
		}

		b, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Failed to marshal: %v", err)
		}

		var decoded URLDeletedEvent
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatalf("Failed to unmarshal: %v", err)
		}

		if original.UserID != decoded.UserID {
			t.Errorf("Expected UserID %v, got %v", original.UserID, decoded.UserID)
		}
	})

	t.Run("MilestoneReachedEvent", func(t *testing.T) {
		original := MilestoneReachedEvent{
			BaseEvent:   NewBaseEvent(EventTypeMilestoneReached, corrID),
			ShortCode:   "abcd123",
			UserID:      "user-1",
			UserEmail:   "user@example.com",
			MilestoneN:  10,
			TotalClicks: 15,
		}

		b, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Failed to marshal: %v", err)
		}

		var decoded MilestoneReachedEvent
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatalf("Failed to unmarshal: %v", err)
		}

		if original.MilestoneN != decoded.MilestoneN {
			t.Errorf("Expected MilestoneN %v, got %v", original.MilestoneN, decoded.MilestoneN)
		}
	})
}
