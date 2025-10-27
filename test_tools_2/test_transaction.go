package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/shopspring/decimal"
)

// =========================
// Domain types (balances)
// =========================

type ChangeKind string

const (
	KindFee       ChangeKind = "Fee"
	KindSwap      ChangeKind = "Swap"
	KindDexOffer  ChangeKind = "DexOffer"
	KindTransfer  ChangeKind = "Transfer"
	KindBurn      ChangeKind = "Burn"
	KindLiquidity ChangeKind = "Liquidity"
	KindPayout    ChangeKind = "Payout"
	KindUnknown   ChangeKind = "Unknown"
)

type BalanceChange struct {
	Account  string
	Currency string
	Issuer   string
	Delta    decimal.Decimal
	Kind     ChangeKind
}

var (
	eps           = decimal.NewFromFloat(1e-9)
	dustThreshold = decimal.NewFromFloat(1e-12) // всё меньше — считаем нулём
	maxIOUValue   = decimal.NewFromFloat(1e20)  // всё больше — считаем мусором
)

// normalizeAmount ограничивает диапазон и убирает "пыль" IOU-значений
func normalizeAmount(val decimal.Decimal) decimal.Decimal {
	abs := val.Abs()

	if abs.LessThan(dustThreshold) {
		return decimal.Zero
	}

	if abs.GreaterThan(maxIOUValue) {
		return decimal.Zero
	}

	return val
}

// ExtractBalanceChanges собирает реальные изменения балансов (XRP + IOU)
// и классифицирует их по типу действия (Fee / Swap / DexOffer / Transfer)
func ExtractBalanceChanges(base map[string]interface{}) []BalanceChange {
	var result []BalanceChange

	meta, ok := base["meta"].(map[string]interface{})
	if !ok {
		return result
	}

	nodes, ok := meta["AffectedNodes"].([]interface{})
	if !ok {
		return result
	}

	txAccount, _ := base["Account"].(string)
	txDestination, _ := base["Destination"].(string)

	// Парсим Fee, который может быть разных типов
	txFee := decimal.Zero
	switch v := base["Fee"].(type) {
	case string:
		txFee, _ = decimal.NewFromString(v)
	case float64:
		txFee = decimal.NewFromFloat(v)
	case int64:
		txFee = decimal.NewFromInt(v)
	case int:
		txFee = decimal.NewFromInt(int64(v))
	case json.Number:
		if f, err := v.Float64(); err == nil {
			txFee = decimal.NewFromFloat(f)
		}
	}

	// 🔹 1) Собираем все аккаунты с AMMID (исключаем их из балансов)
	ammAccounts := map[string]bool{}
	for _, raw := range nodes {
		nodeMap, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		for _, nodeType := range []string{"ModifiedNode", "DeletedNode", "CreatedNode"} {
			node, ok := nodeMap[nodeType].(map[string]interface{})
			if !ok {
				continue
			}
			if ledgerType, _ := node["LedgerEntryType"].(string); ledgerType != "AccountRoot" {
				continue
			}
			final, _ := node["FinalFields"].(map[string]interface{})
			if final == nil {
				final, _ = node["NewFields"].(map[string]interface{})
			}
			if final == nil {
				continue
			}
			if _, ok := final["AMMID"]; ok {
				if acc, _ := final["Account"].(string); acc != "" {
					ammAccounts[acc] = true
				}
			}
		}
	}

	// 🔹 2) Проверяем наличие Offer (DEX)
	hasOffer := false
	for _, raw := range nodes {
		nodeMap, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		for _, nodeType := range []string{"ModifiedNode", "DeletedNode", "CreatedNode"} {
			node, ok := nodeMap[nodeType].(map[string]interface{})
			if !ok {
				continue
			}
			if ledgerType, _ := node["LedgerEntryType"].(string); ledgerType == "Offer" {
				hasOffer = true
				break
			}
		}
	}

	// 🔹 3) Проверяем self-swap
	isSelfSwap := txAccount != "" && txAccount == txDestination && !hasOffer

	// 🔹 4) Собираем изменения
	for _, raw := range nodes {
		nodeMap, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}

		// Обработка ModifiedNode и DeletedNode
		for _, nodeType := range []string{"ModifiedNode", "DeletedNode"} {
			node, ok := nodeMap[nodeType].(map[string]interface{})
			if !ok {
				continue
			}

			ledgerType, _ := node["LedgerEntryType"].(string)
			final, _ := node["FinalFields"].(map[string]interface{})
			prev, _ := node["PreviousFields"].(map[string]interface{})
			if final == nil || prev == nil {
				continue
			}

			switch ledgerType {

			case "AccountRoot":
				account, _ := final["Account"].(string)
				balFinalStr, _ := final["Balance"].(string)
				balPrevStr, _ := prev["Balance"].(string)

				if account == "" || balFinalStr == "" || balPrevStr == "" {
					continue
				}
				if ammAccounts[account] || (isSelfSwap && account != txAccount) {
					continue
				}

				balFinal, _ := decimal.NewFromString(balFinalStr)
				balPrev, _ := decimal.NewFromString(balPrevStr)
				delta := normalizeAmount(balFinal.Sub(balPrev).Div(decimal.NewFromInt(1_000_000)))

				if delta.IsZero() {
					continue
				}

				kind := KindUnknown

				// Определяем Fee по точному совпадению с суммой комиссии
				if account == txAccount && delta.IsNegative() && delta.Abs().Equal(txFee.Div(decimal.NewFromInt(1_000_000)).Abs()) {
					kind = KindFee
				}

				result = append(result, BalanceChange{
					Account:  account,
					Currency: "XRP",
					Issuer:   "XRP",
					Delta:    delta,
					Kind:     kind,
				})

			case "RippleState":
				balFinal, ok1 := extractDecimal(final, "Balance", "value")
				balPrev, ok2 := extractDecimal(prev, "Balance", "value")
				if !ok1 || !ok2 {
					continue
				}
				delta := normalizeAmount(balFinal.Sub(balPrev))
				if delta.IsZero() {
					continue
				}

				highLimit, _ := final["HighLimit"].(map[string]interface{})
				lowLimit, _ := final["LowLimit"].(map[string]interface{})
				highIssuer, _ := highLimit["issuer"].(string)
				lowIssuer, _ := lowLimit["issuer"].(string)
				currency, _ := highLimit["currency"].(string)
				tokenIssuer := detectTokenIssuer(base, currency)

				// Определяем burn операцию: если токены отправляются эмитенту
				isBurn := false
				if txDestination != "" && txDestination == tokenIssuer && delta.IsPositive() {
					isBurn = true
				}
				
				// Определяем выдачу: если эмитент отправляет токен пользователю
				isPayout := false
				if txAccount == tokenIssuer && delta.IsPositive() && txDestination != tokenIssuer {
					isPayout = true
				}

				// сторона High (баланс ведётся от лица HighIssuer)
				if highIssuer != "" && highIssuer != tokenIssuer && !ammAccounts[highIssuer] && !(isSelfSwap && highIssuer != txAccount) {
					kind := KindUnknown
					if isBurn && highIssuer == txAccount {
						kind = KindBurn
					}
					result = append(result, BalanceChange{
						Account:  highIssuer,
						Currency: currency,
						Issuer:   lowIssuer,
						Delta:    delta.Neg(),
						Kind:     kind,
					})
				}
				// сторона Low
				if lowIssuer != "" && lowIssuer != tokenIssuer && !ammAccounts[lowIssuer] && !(isSelfSwap && lowIssuer != txAccount) {
					kind := KindUnknown
					if isBurn && lowIssuer == txAccount {
						kind = KindBurn
					}
					if isPayout && lowIssuer == txDestination {
						kind = KindPayout
					}
					result = append(result, BalanceChange{
						Account:  lowIssuer,
						Currency: currency,
						Issuer:   highIssuer,
						Delta:    delta,
						Kind:     kind,
					})
				}
			}
		}

		// Обработка CreatedNode (создание новых аккаунтов или trustlines)
		if node, ok := nodeMap["CreatedNode"].(map[string]interface{}); ok {
			ledgerType, _ := node["LedgerEntryType"].(string)
			newFields, _ := node["NewFields"].(map[string]interface{})
			if newFields == nil {
				continue
			}

			switch ledgerType {

			case "AccountRoot":
				account, _ := newFields["Account"].(string)
				balNewStr, _ := newFields["Balance"].(string)

				if account == "" || balNewStr == "" {
					continue
				}
				if ammAccounts[account] || (isSelfSwap && account != txAccount) {
					continue
				}

				balNew, _ := decimal.NewFromString(balNewStr)
				delta := normalizeAmount(balNew.Div(decimal.NewFromInt(1_000_000)))

				if delta.IsZero() {
					continue
				}

				// Новый аккаунт получает баланс (всегда положительный)
				result = append(result, BalanceChange{
					Account:  account,
					Currency: "XRP",
					Issuer:   "XRP",
					Delta:    delta,
					Kind:     KindUnknown,
				})

			case "RippleState":
				// Создание trustline с начальным балансом
				balNew, ok1 := extractDecimal(newFields, "Balance", "value")
				if !ok1 || balNew.IsZero() {
					continue
				}

				highLimit, _ := newFields["HighLimit"].(map[string]interface{})
				lowLimit, _ := newFields["LowLimit"].(map[string]interface{})
				highIssuer, _ := highLimit["issuer"].(string)
				lowIssuer, _ := lowLimit["issuer"].(string)
				currency, _ := highLimit["currency"].(string)
				tokenIssuer := detectTokenIssuer(base, currency)

				// сторона High
				if highIssuer != "" && highIssuer != tokenIssuer && !ammAccounts[highIssuer] && !(isSelfSwap && highIssuer != txAccount) {
					result = append(result, BalanceChange{
						Account:  highIssuer,
						Currency: currency,
						Issuer:   lowIssuer,
						Delta:    balNew.Neg(),
						Kind:     KindUnknown,
					})
				}
				// сторона Low
				if lowIssuer != "" && lowIssuer != tokenIssuer && !ammAccounts[lowIssuer] && !(isSelfSwap && lowIssuer != txAccount) {
					result = append(result, BalanceChange{
						Account:  lowIssuer,
						Currency: currency,
						Issuer:   highIssuer,
						Delta:    balNew,
						Kind:     KindUnknown,
					})
				}
			}
		}
	}

	return result
}

// detectTokenIssuer определяет эмитента токена из полей Amount / SendMax / DeliveredAmount
func detectTokenIssuer(tx map[string]interface{}, currency string) string {
	check := func(v interface{}) string {
		if m, ok := v.(map[string]interface{}); ok {
			if c, _ := m["currency"].(string); c == currency {
				if iss, _ := m["issuer"].(string); iss != "" {
					return iss
				}
			}
		}
		return ""
	}
	if iss := check(tx["Amount"]); iss != "" {
		return iss
	}
	if iss := check(tx["SendMax"]); iss != "" {
		return iss
	}
	if meta, ok := tx["meta"].(map[string]interface{}); ok {
		if iss := check(meta["DeliveredAmount"]); iss != "" {
			return iss
		}
		if iss := check(meta["delivered_amount"]); iss != "" {
			return iss
		}
	}
	return ""
}

func extractDecimal(m map[string]interface{}, key string, innerKey string) (decimal.Decimal, bool) {
	v, ok := m[key].(map[string]interface{})
	if !ok {
		return decimal.Zero, false
	}
	valStr, _ := v[innerKey].(string)
	if valStr == "" {
		return decimal.Zero, false
	}
	val, err := decimal.NewFromString(valStr)
	if err != nil {
		return decimal.Zero, false
	}
	return normalizeAmount(val), true
}

// =========================
// Grouping into Actions
// =========================

type ActionKind string

const (
	ActTransfer  ActionKind = "Transfer"
	ActSwap      ActionKind = "Swap"      // одно лицо: -X +Y
	ActDexOffer  ActionKind = "DexOffer"  // одно лицо: -A +B (не отправитель/получатель)
	ActFee       ActionKind = "Fee"       // -XRP комиссия
	ActBurn      ActionKind = "Burn"      // сжигание токенов (отправка эмитенту)
	ActLiquidity ActionKind = "Liquidity" // вывод ликвидности (депозит в AMM)
)

type Side struct {
	Account  string
	Currency string
	Issuer   string
	Amount   decimal.Decimal // + получено, - отправлено
}

type Action struct {
	Kind  ActionKind
	Sides []Side // 1..2 стороны (fee=1, swap/dexOffer=2, transfer=2)
	Note  string // опционально, для отладки/вывода
}

func sign(x decimal.Decimal) int {
	switch {
	case x.GreaterThan(eps):
		return +1
	case x.LessThan(eps.Neg()):
		return -1
	default:
		return 0
	}
}

// getDeliveredInfo: доставленная валюта/эмитент/значение (учитывает XRP как строку в дропах)
func getDeliveredInfo(tx map[string]interface{}) (currency, issuer string, value decimal.Decimal, isIOU bool, ok bool) {
	tryObj := func(v interface{}) (string, string, decimal.Decimal, bool) {
		m, _ := v.(map[string]interface{})
		if m == nil {
			return "", "", decimal.Zero, false
		}
		cur, _ := m["currency"].(string)
		iss, _ := m["issuer"].(string)
		valStr, _ := m["value"].(string)
		if cur == "" || valStr == "" {
			return "", "", decimal.Zero, false
		}
		val, err := decimal.NewFromString(valStr)
		if err != nil {
			return "", "", decimal.Zero, false
		}
		return cur, iss, val, true
	}

	// meta.delivered_amount / DeliveredAmount
	if meta, okm := tx["meta"].(map[string]interface{}); okm {
		if cur, iss, v, okk := tryObj(meta["delivered_amount"]); okk {
			return cur, iss, v, true, true
		}
		if cur, iss, v, okk := tryObj(meta["DeliveredAmount"]); okk {
			return cur, iss, v, true, true
		}
		// XRP case: может быть строкой-дропами
		if s, oks := meta["delivered_amount"].(string); oks && s != "" {
			if drops, err := decimal.NewFromString(s); err == nil {
				return "XRP", "XRP", drops.Div(decimal.NewFromInt(1_000_000)), false, true
			}
		}
		if s, oks := meta["DeliveredAmount"].(string); oks && s != "" {
			if drops, err := decimal.NewFromString(s); err == nil {
				return "XRP", "XRP", drops.Div(decimal.NewFromInt(1_000_000)), false, true
			}
		}
	}

	// fallback к Amount (если нет delivered_amount)
	switch a := tx["Amount"].(type) {
	case string: // XRP drops
		if a != "" {
			if drops, err := decimal.NewFromString(a); err == nil {
				return "XRP", "XRP", drops.Div(decimal.NewFromInt(1_000_000)), false, true
			}
		}
	case map[string]interface{}:
		if cur, iss, v, okk := tryObj(a); okk {
			return cur, iss, v, true, true
		}
	}

	return "", "", decimal.Zero, false, false
}

// собирает change индексы по аккаунту, исключая уже использованные
func collectByAccount(changes []BalanceChange, used []bool) map[string][]int {
	m := make(map[string][]int)
	for i := range changes {
		if used[i] {
			continue
		}
		m[changes[i].Account] = append(m[changes[i].Account], i)
	}
	return m
}

// подбирает у аккаунта пары (+/-) как одно действие.
// если thisKind=ActSwap — помечаем Kind у парных на Swap, если ActDexOffer — DexOffer.
func pairAccountActions(
	account string,
	changes []BalanceChange,
	used []bool,
	actionKind ActionKind,
) (acts []Action) {

	var idxs []int
	for i := range changes {
		if used[i] {
			continue
		}
		if changes[i].Account == account {
			idxs = append(idxs, i)
		}
	}
	if len(idxs) == 0 {
		return
	}

	var pos, neg []int
	for _, i := range idxs {
		if sign(changes[i].Delta) > 0 {
			pos = append(pos, i)
		} else if sign(changes[i].Delta) < 0 {
			neg = append(neg, i)
		}
	}

	// Жадно парим самый большой по модулю с самым большим по модулю
	sort.Slice(pos, func(i, j int) bool {
		return changes[pos[i]].Delta.GreaterThan(changes[pos[j]].Delta)
	})
	sort.Slice(neg, func(i, j int) bool {
		return changes[neg[i]].Delta.Neg().GreaterThan(changes[neg[j]].Delta.Neg())
	})

	n := len(pos)
	if len(neg) < n {
		n = len(neg)
	}
	for k := 0; k < n; k++ {
		ip := pos[k]
		in := neg[k]
		acts = append(acts, Action{
			Kind: actionKind,
			Sides: []Side{
				{
					Account:  account,
					Currency: changes[in].Currency,
					Issuer:   changes[in].Issuer,
					Amount:   changes[in].Delta, // минус
				},
				{
					Account:  account,
					Currency: changes[ip].Currency,
					Issuer:   changes[ip].Issuer,
					Amount:   changes[ip].Delta, // плюс
				},
			},
			Note: fmt.Sprintf("%s pair for %s", actionKind, account),
		})
		used[ip] = true
		used[in] = true
		// проставим Kind у изменений
		switch actionKind {
		case ActSwap:
			changes[ip].Kind = KindSwap
			changes[in].Kind = KindSwap
		case ActDexOffer:
			changes[ip].Kind = KindDexOffer
			changes[in].Kind = KindDexOffer
		}
	}

	return
}

// BuildActionGroups — формирует связки действий из списка изменённых балансов
func BuildActionGroups(tx map[string]interface{}, changes []BalanceChange) []Action {
	var actions []Action
	used := make([]bool, len(changes))

	txAccount, _ := tx["Account"].(string)
	txDestination, _ := tx["Destination"].(string)

	// 1) Fee (явное)
	for i := range changes {
		if changes[i].Kind == KindFee && !used[i] {
			actions = append(actions, Action{
				Kind: ActFee,
				Sides: []Side{{
					Account:  changes[i].Account,
					Currency: "XRP",
					Issuer:   "XRP",
					Amount:   changes[i].Delta, // отрицательное
				}},
				Note: "Fee",
			})
			used[i] = true
		}
	}

	// 2) Burn (явное)
	for i := range changes {
		if changes[i].Kind == KindBurn && !used[i] {
			actions = append(actions, Action{
				Kind: ActBurn,
				Sides: []Side{{
					Account:  changes[i].Account,
					Currency: changes[i].Currency,
					Issuer:   changes[i].Issuer,
					Amount:   changes[i].Delta, // отрицательное (токены сжигаются)
				}},
				Note: "Burn",
			})
			used[i] = true
		}
	}

	// 3) Liquidity (явное)
	for i := range changes {
		if changes[i].Kind == KindLiquidity && !used[i] {
			actions = append(actions, Action{
				Kind: ActLiquidity,
				Sides: []Side{{
					Account:  changes[i].Account,
					Currency: "XRP",
					Issuer:   "XRP",
					Amount:   changes[i].Delta,
				}},
				Note: "Liquidity",
			})
			used[i] = true
		}
	}

	// 4) Свап у отправителя (если есть -XRP и +IOU/XRP)
	if txAccount != "" {
		acts := pairAccountActions(txAccount, changes, used, ActSwap)
		actions = append(actions, acts...)
	}

	// 5) DexOffer у остальных (каждый аккаунт, кроме отправителя и получателя)
	byAcct := collectByAccount(changes, used)
	for acct := range byAcct {
		if acct == txAccount || acct == txDestination {
			continue
		}
		acts := pairAccountActions(acct, changes, used, ActDexOffer)
		actions = append(actions, acts...)
	}

	// 6) Transfer: связать txAccount(оставшийся минус) -> txDestination(плюс delivered)
	delCur, delIss, delVal, _, hasDel := getDeliveredInfo(tx)

	var destPosIdx = -1
	if hasDel && txDestination != "" {
		// ищем у destination положительный change по delivered (лучший матч по валюте/эмитенту/величине)
		bestDiff := decimal.NewFromFloat(1e18)
		for i := range changes {
			if used[i] {
				continue
			}
			c := changes[i]
			if c.Account != txDestination || sign(c.Delta) <= 0 {
				continue
			}
			if delCur == "XRP" && c.Currency == "XRP" {
				diff := c.Delta.Sub(delVal).Abs()
				if diff.LessThan(bestDiff) {
					bestDiff = diff
					destPosIdx = i
				}
			} else if c.Currency == delCur && c.Issuer == delIss {
				diff := c.Delta.Sub(delVal).Abs()
				if diff.LessThan(bestDiff) {
					bestDiff = diff
					destPosIdx = i
				}
			}
		}
	}

	var senderNegIdx = -1
	if txAccount != "" {
		// берём крупнейший (по модулю) отрицательный остаток у отправителя (после свапа/фии)
		best := decimal.Zero
		for i := range changes {
			if used[i] {
				continue
			}
			c := changes[i]
			if c.Account == txAccount && sign(c.Delta) < 0 && c.Delta.Abs().GreaterThan(best) {
				best = c.Delta.Abs()
				senderNegIdx = i
			}
		}
	}

	if senderNegIdx >= 0 && destPosIdx >= 0 {
		actions = append(actions, Action{
			Kind: ActTransfer,
			Sides: []Side{
				{
					Account:  changes[senderNegIdx].Account,
					Currency: changes[senderNegIdx].Currency,
					Issuer:   changes[senderNegIdx].Issuer,
					Amount:   changes[senderNegIdx].Delta, // -
				},
				{
					Account:  changes[destPosIdx].Account,
					Currency: changes[destPosIdx].Currency,
					Issuer:   changes[destPosIdx].Issuer,
					Amount:   changes[destPosIdx].Delta, // +
				},
			},
			Note: "Transfer (sender->destination, possibly cross-currency)",
		})
		used[senderNegIdx] = true
		used[destPosIdx] = true
		// пометим Kind у изменений
		changes[senderNegIdx].Kind = KindTransfer
		changes[destPosIdx].Kind = KindTransfer
	}

	// 6) Остатки — как Unknown (редкие случаи, например, частичные/мульти-пары)
	for i := range changes {
		if used[i] {
			continue
		}
		actions = append(actions, Action{
			Kind: ActLiquidity,
			Sides: []Side{{
				Account:  changes[i].Account,
				Currency: changes[i].Currency,
				Issuer:   changes[i].Issuer,
				Amount:   changes[i].Delta,
			}},
			Note: "Unpaired residue",
		})
	}

	return actions
}

// =========================
// Utils for printing
// =========================

func fmtAmt(s Side) string {
	unit := s.Currency
	if unit == "XRP" {
		unit = "XRP"
	} else {
		// короткая форма HEX кода: первые 8 символов и эмитент в короткой нотации
		if len(unit) > 8 {
			unit = unit[:8] + "…"
		}
	}
	iss := s.Issuer
	if iss == "" {
		iss = "-"
	}
	sign := "+"
	if s.Amount.IsNegative() {
		sign = "-"
	}
	return fmt.Sprintf("%s %.6f %s.%s",
		sign, s.Amount.Abs().InexactFloat64(), unit, shortAddr(iss))
}

func shortAddr(a string) string {
	if a == "" || a == "XRP" {
		return a
	}
	if len(a) <= 6 {
		return a
	}
	return a[:3] + "…" + a[len(a)-3:]
}

// =========================
// Main
// =========================
func main() {
	// Обрабатываем файлы от tx_1.json до tx_14.json
	for i := 23; i <= 23; i++ {
		filename := fmt.Sprintf("../examples/tx_%d.json", i)

		fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
		fmt.Printf("ОБРАБОТКА ФАЙЛА: %s\n", filename)
		fmt.Printf(strings.Repeat("=", 80) + "\n")

		data, err := os.ReadFile(filename)
		if err != nil {
			fmt.Printf("Ошибка чтения файла %s: %v\n", filename, err)
			continue
		}

		var tx map[string]interface{}
		if err := json.Unmarshal(data, &tx); err != nil {
			fmt.Printf("Ошибка парсинга JSON файла %s: %v\n", filename, err)
			continue
		}

		// 1) Плоские изменения
		changes := ExtractBalanceChanges(tx)

		// 2) Связки действий (помечают Kind у changes на Swap/DexOffer/Transfer/и т.д.)
		actions := BuildActionGroups(tx, changes)

		// 3) Печать изменений (уже с поставленными Kind)
		fmt.Println("=== Изменения балансов ===")
		for j, c := range changes {
			sign := "+"
			if c.Delta.IsNegative() {
				sign = "-"
			}
			fmt.Printf("%2d. %-35s %6s %s %s%.6f   [%s]\n",
				j+1, c.Account, c.Currency, c.Issuer, sign, c.Delta.Abs().InexactFloat64(), c.Kind)
		}
		fmt.Printf("Всего записей: %d\n", len(changes))

		// 4) Печать связок действий
		fmt.Println("\n=== Связки действий ===")
		for j, a := range actions {
			var sb []string
			for _, s := range a.Sides {
				sb = append(sb, fmt.Sprintf("%s: %s", shortAddr(s.Account), fmtAmt(s)))
			}
			note := a.Note
			if note != "" {
				note = "  // " + note
			}
			fmt.Printf("%2d) %-9s  %s%s\n", j+1, a.Kind, strings.Join(sb, " | "), note)
		}
	}
}
