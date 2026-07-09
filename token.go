package main

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/google/uuid"
)

// Claims — JWT token'ining foydali yuki: ruxsat etilgan task kind'lari + standart claim'lar.
type Claims struct {
	Kinds []string `json:"kinds"`
	jwt.RegisteredClaims
}

// TokenRecord — registry'da saqlanadigan token yozuvi (token satrini emas, faqat
// metama'lumotni saqlaymiz — jti, kind'lar, muddat, revoke holati).
type TokenRecord struct {
	JTI     string    `json:"jti"`
	Kinds   []string  `json:"kinds"`
	Created time.Time `json:"created"`
	Expires time.Time `json:"expires,omitempty"`
	Revoked bool      `json:"revoked"`
}

// tokenFile — registry'ning disk formati.
type tokenFile struct {
	Tokens []TokenRecord `json:"tokens"`
}

// TokenRegistry — chiqarilgan tokenlar reyestri (disk'dagi JSON fayl).
//
// Token amal qilishi uchun: (1) imzo TOKEN_SECRET bilan to'g'ri, (2) muddati
// o'tmagan, (3) jti registry'da mavjud va revoke qilinmagan. Bu "fail-closed"
// yondashuv — registry yo'qolsa yoki jti topilmasa token ishlamaydi, shu bois
// revoke har doim ishonchli. CLI va server bir xil TOKEN_STORE va TOKEN_SECRET'ni
// baham ko'rishi kerak.
type TokenRegistry struct {
	path   string
	secret []byte

	mu      sync.Mutex
	records map[string]TokenRecord
}

// NewTokenRegistry registry'ni ochadi (fayl bo'lmasa — bo'sh reyestr).
func NewTokenRegistry(path string, secret []byte) *TokenRegistry {
	reg := &TokenRegistry{path: path, secret: secret, records: make(map[string]TokenRecord)}
	reg.mu.Lock()
	reg.loadLocked()
	reg.mu.Unlock()
	return reg
}

// Issue yangi token chiqaradi: JWT'ni imzolaydi va yozuvni registry'ga qo'shadi.
// ttl 0 bo'lsa token muddatsiz.
func (reg *TokenRegistry) Issue(kinds []string, ttl time.Duration) (string, TokenRecord, error) {
	now := time.Now()
	rec := TokenRecord{JTI: uuid.NewString(), Kinds: kinds, Created: now}

	claims := Claims{
		Kinds: kinds,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:       rec.JTI,
			IssuedAt: jwt.NewNumericDate(now),
		},
	}
	if ttl > 0 {
		rec.Expires = now.Add(ttl)
		claims.ExpiresAt = jwt.NewNumericDate(rec.Expires)
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(reg.secret)
	if err != nil {
		return "", TokenRecord{}, err
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if err := reg.loadLocked(); err != nil {
		return "", TokenRecord{}, err
	}
	reg.records[rec.JTI] = rec
	if err := reg.saveLocked(); err != nil {
		return "", TokenRecord{}, err
	}
	return signed, rec, nil
}

// Revoke token'ni bekor qilingan deb belgilaydi.
func (reg *TokenRegistry) Revoke(jti string) error {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if err := reg.loadLocked(); err != nil {
		return err
	}
	rec, ok := reg.records[jti]
	if !ok {
		return errors.New("token topilmadi: " + jti)
	}
	if rec.Revoked {
		return nil // allaqachon bekor qilingan
	}
	rec.Revoked = true
	reg.records[jti] = rec
	return reg.saveLocked()
}

// List barcha token yozuvlarini qaytaradi.
func (reg *TokenRegistry) List() ([]TokenRecord, error) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if err := reg.loadLocked(); err != nil {
		return nil, err
	}
	out := make([]TokenRecord, 0, len(reg.records))
	for _, r := range reg.records {
		out = append(out, r)
	}
	return out, nil
}

// Parse JWT'ni tekshiradi (imzo + muddat + revoke) va claim'larni qaytaradi.
func (reg *TokenRegistry) Parse(tokenStr string) (*Claims, error) {
	var claims Claims
	_, err := jwt.ParseWithClaims(tokenStr, &claims, func(*jwt.Token) (any, error) {
		return reg.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		return nil, err
	}
	// Fayldan yangi holatni o'qiymiz — revoke darhol kuchga kirishi uchun.
	reg.mu.Lock()
	err = reg.loadLocked()
	rec, ok := reg.records[claims.ID]
	reg.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("token reyestrda yo'q")
	}
	if rec.Revoked {
		return nil, errors.New("token bekor qilingan")
	}
	return &claims, nil
}

// loadLocked fayldan yozuvlarni o'qiydi (mu ushlab turilgan holda). Fayl yo'q — bo'sh.
func (reg *TokenRegistry) loadLocked() error {
	data, err := os.ReadFile(reg.path)
	if err != nil {
		if os.IsNotExist(err) {
			reg.records = make(map[string]TokenRecord)
			return nil
		}
		return err
	}
	var tf tokenFile
	if err := json.Unmarshal(data, &tf); err != nil {
		return err
	}
	m := make(map[string]TokenRecord, len(tf.Tokens))
	for _, r := range tf.Tokens {
		m[r.JTI] = r
	}
	reg.records = m
	return nil
}

// saveLocked yozuvlarni faylga atomik yozadi (mu ushlab turilgan holda).
func (reg *TokenRegistry) saveLocked() error {
	tf := tokenFile{Tokens: make([]TokenRecord, 0, len(reg.records))}
	for _, r := range reg.records {
		tf.Tokens = append(tf.Tokens, r)
	}
	data, err := json.MarshalIndent(tf, "", "  ")
	if err != nil {
		return err
	}
	tmp := reg.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, reg.path)
}
