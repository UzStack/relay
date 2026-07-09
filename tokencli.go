package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// tokenCLI `relay token <create|revoke|list>` subcommand'larini bajaradi.
// TOKEN_SECRET va TOKEN_STORE env'lari server bilan bir xil bo'lishi kerak.
func tokenCLI(args []string) {
	secret := os.Getenv("TOKEN_SECRET")
	if secret == "" {
		fatal("TOKEN_SECRET env o'rnatilishi shart")
	}
	store := getenv("TOKEN_STORE", "relay-tokens.json")
	reg := NewTokenRegistry(store, []byte(secret))

	if len(args) == 0 {
		tokenUsage()
		os.Exit(2)
	}

	switch args[0] {
	case "create":
		tokenCreate(reg, args[1:])
	case "revoke":
		tokenRevoke(reg, args[1:])
	case "list":
		tokenList(reg)
	default:
		tokenUsage()
		os.Exit(2)
	}
}

func tokenCreate(reg *TokenRegistry, args []string) {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	kinds := fs.String("kinds", "", "vergul bilan ajratilgan ruxsat etilgan kind'lar (masalan: http,email)")
	ttl := fs.Duration("ttl", 0, "amal qilish muddati (masalan 720h); 0 = muddatsiz")
	fs.Parse(args)

	kindList := splitKinds(*kinds)
	if len(kindList) == 0 {
		fatal("--kinds shart (masalan: --kinds http,email)")
	}

	token, rec, err := reg.Issue(kindList, *ttl)
	if err != nil {
		fatal("token yaratish: %v", err)
	}
	fmt.Printf("jti:     %s\n", rec.JTI)
	fmt.Printf("kinds:   %s\n", strings.Join(rec.Kinds, ","))
	fmt.Printf("expires: %s\n\n", expStr(rec.Expires))
	fmt.Printf("token (bir marta ko'rsatiladi):\n%s\n", token)
}

func tokenRevoke(reg *TokenRegistry, args []string) {
	if len(args) != 1 {
		fatal("foydalanish: relay token revoke <jti>")
	}
	if err := reg.Revoke(args[0]); err != nil {
		fatal("revoke: %v", err)
	}
	fmt.Printf("bekor qilindi: %s\n", args[0])
}

func tokenList(reg *TokenRegistry) {
	recs, err := reg.List()
	if err != nil {
		fatal("list: %v", err)
	}
	if len(recs) == 0 {
		fmt.Println("token yo'q")
		return
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Created.After(recs[j].Created) })
	fmt.Printf("%-36s  %-20s  %-10s  %s\n", "JTI", "KINDS", "HOLAT", "MUDDAT")
	for _, r := range recs {
		status := "active"
		if r.Revoked {
			status = "revoked"
		} else if !r.Expires.IsZero() && r.Expires.Before(time.Now()) {
			status = "expired"
		}
		fmt.Printf("%-36s  %-20s  %-10s  %s\n", r.JTI, strings.Join(r.Kinds, ","), status, expStr(r.Expires))
	}
}

func tokenUsage() {
	fmt.Fprintln(os.Stderr, `foydalanish: relay token <command>

  create --kinds http,email [--ttl 720h]   yangi scoped token yaratish
  revoke <jti>                             token'ni bekor qilish
  list                                     token'lar ro'yxati

Env: TOKEN_SECRET (majburiy), TOKEN_STORE (default: relay-tokens.json)`)
}

// splitKinds "http, email" kabi satrni tozalab bo'laklarga ajratadi.
func splitKinds(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func expStr(t time.Time) string {
	if t.IsZero() {
		return "muddatsiz"
	}
	return t.Format(time.RFC3339)
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "xato: "+format+"\n", a...)
	os.Exit(1)
}
