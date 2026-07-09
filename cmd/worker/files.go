package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"
)

// files — relay'ning /files endpoint'lari bilan ishlovchi global klient.
// main() da sozlanadi; fayl handler'lari shu orqali yuklab oladi/yuklaydi.
var files *fileClient

// fileClient relay'ning /files HTTP endpoint'lariga so'rov yuboradi.
type fileClient struct {
	baseURL string // masalan http://localhost:8080
	token   string
	http    *http.Client
}

// newFileClient WS URL'idan HTTP baza manzilini keltirib chiqaradi
// (ws://host/ws → http://host, wss:// → https://).
func newFileClient(wsURL, token string) *fileClient {
	base := strings.TrimSuffix(wsURL, "/ws")
	base = strings.Replace(base, "wss://", "https://", 1)
	base = strings.Replace(base, "ws://", "http://", 1)
	return &fileClient{
		baseURL: base,
		token:   token,
		http:    &http.Client{Timeout: 60 * time.Second},
	}
}

// Download file_id bo'yicha faylni relay'dan yuklab oladi.
func (c *fileClient) Download(fileID string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+"/files/"+fileID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fayl yuklab olinmadi: status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// Upload baytlarni relay'ga yuklaydi va yangi file_id qaytaradi.
func (c *fileClient) Upload(filename, contentType string, data []byte) (string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	part, err := mw.CreatePart(h)
	if err != nil {
		return "", err
	}
	if _, err := part.Write(data); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}

	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/files", &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("fayl yuklanmadi: status %d", resp.StatusCode)
	}
	var meta struct {
		FileID string `json:"file_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return "", err
	}
	return meta.FileID, nil
}
