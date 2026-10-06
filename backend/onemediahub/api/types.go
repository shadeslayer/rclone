// Package api defines OneMediaHub Server API messages.
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// ID accepts the numeric and string identifiers used by SAPI.
type ID string

// UnmarshalJSON decodes a numeric or quoted identifier.
func (id *ID) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte("null")) {
		*id = ""
		return nil
	}
	var s string
	if len(b) > 0 && b[0] == '"' {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	} else {
		s = string(b)
	}
	if s != "" {
		if _, err := strconv.ParseUint(s, 10, 64); err != nil {
			return fmt.Errorf("invalid SAPI identifier: %w", err)
		}
	}
	*id = ID(s)
	return nil
}

// MarshalJSON encodes identifiers as numbers for folder and item requests.
func (id ID) MarshalJSON() ([]byte, error) {
	if id == "" {
		return []byte("null"), nil
	}
	if _, err := strconv.ParseUint(string(id), 10, 64); err != nil {
		return nil, err
	}
	return []byte(id), nil
}

// Error describes a SAPI failure, including failures returned with HTTP 200.
type Error struct {
	Code    string `json:"code"`    // Code identifies the failure.
	Message string `json:"message"` // Message describes the failure.
}

// Error returns the server error code and message.
func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Response contains the common response envelope.
type Response struct {
	RequestTime json.Number     `json:"requesttime"` // RequestTime is the server response time in milliseconds.
	Data        json.RawMessage `json:"data"`        // Data contains the operation result.
	Error       *Error          `json:"error"`       // Error contains a failure, if any.
	ID          ID              `json:"id"`          // ID identifies a saved item or folder.
	More        bool            `json:"more"`        // More indicates another media page.
}

// Session contains the credentials for a SAPI session.
type Session struct {
	ID  string `json:"jsessionid"`    // ID is the JSESSIONID cookie value.
	Key string `json:"validationkey"` // Key is the CSRF validation key.
}

// Folder describes a directory.
type Folder struct {
	ID          ID     `json:"id,omitempty"`          // ID identifies the folder.
	ParentID    ID     `json:"parentid,omitempty"`    // ParentID identifies its parent.
	Name        string `json:"name"`                  // Name is the folder name.
	Date        int64  `json:"date,omitempty"`        // Date is the server update time in milliseconds.
	Status      string `json:"status,omitempty"`      // Status is the server item status.
	SoftDeleted bool   `json:"softdeleted,omitempty"` // SoftDeleted indicates a trashed folder.
}

// IsDeleted reports whether the folder is deleted or in the trash.
func (f Folder) IsDeleted() bool {
	return f.SoftDeleted || f.Status == "D" || f.Status == "S"
}

// Changes contains changed item IDs grouped by their server status.
type Changes struct {
	New     []ID `json:"N,omitempty"` // New identifies added items.
	Updated []ID `json:"U,omitempty"` // Updated identifies modified items.
	Deleted []ID `json:"D,omitempty"` // Deleted identifies removed items.
	Locked  []ID `json:"L,omitempty"` // Locked identifies items being processed.
}

// Media describes a stored object.
type Media struct {
	ID          ID     `json:"id"`               // ID identifies the media item.
	FolderID    ID     `json:"folderid"`         // FolderID identifies the parent folder.
	Folder      ID     `json:"folder"`           // Folder is the older generic-media parent field.
	Name        string `json:"name"`             // Name is the filename.
	Size        int64  `json:"size"`             // Size is the content length.
	Modified    int64  `json:"modificationdate"` // Modified is the modification time in milliseconds.
	Date        int64  `json:"date"`             // Date is the server update time in milliseconds.
	URL         string `json:"url"`              // URL downloads the original content.
	ETag        string `json:"etag"`             // ETag is an opaque server content version.
	Type        string `json:"mediatype"`        // Type is file, picture, video, or audio.
	Status      string `json:"status"`           // Status is the upload status.
	SoftDeleted bool   `json:"softdeleted"`      // SoftDeleted indicates a trashed item.
}

// IsDeleted reports whether the media item is deleted or in the trash.
func (m Media) IsDeleted() bool {
	return m.SoftDeleted || m.Status == "D" || m.Status == "S"
}

// Upload describes content being created or replaced.
type Upload struct {
	ID          string `json:"id,omitempty"`       // ID identifies an existing item for replacement.
	FolderID    ID     `json:"folderid,omitempty"` // FolderID identifies the destination folder.
	Name        string `json:"name"`               // Name is the filename.
	Size        int64  `json:"size"`               // Size is the content length.
	ContentType string `json:"contenttype"`        // ContentType is the MIME type.
	Created     string `json:"creationdate"`       // Created is an RFC 2445 UTC date.
	Modified    string `json:"modificationdate"`   // Modified is an RFC 2445 UTC date.
}

// MetadataUpdate describes a file rename, parent move, or timestamp update without content replacement.
type MetadataUpdate struct {
	ID       string `json:"id"`               // ID identifies the existing media item.
	FolderID ID     `json:"folderid"`         // FolderID identifies the destination parent, with zero for the root.
	Name     string `json:"name"`             // Name is the filename.
	Modified string `json:"modificationdate"` // Modified is an RFC 2445 UTC date.
}

// ServerInfo contains public deployment settings.
type ServerInfo struct {
	UploadURL string `json:"sapi.upload.endpoint"` // UploadURL is the dedicated upload server, if any.
	Error     *Error `json:"error"`                // Error contains a discovery failure, if any.
}
