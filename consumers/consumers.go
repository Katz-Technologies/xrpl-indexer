package consumers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

const BLACKLIST_ACCOUNT_ROGUE = "rogue5HnPRSszD9CWGSUz8UGHMVwSSKF6"

var (
	consumerWg      sync.WaitGroup
	consumerWgMutex sync.Mutex
	consumerActive  bool
)

type ChangeKind string

const (
	KindFee      ChangeKind = "Fee"
	KindSwap     ChangeKind = "Swap"
	KindDexOffer ChangeKind = "DexOffer"
	KindTransfer ChangeKind = "Transfer"
	KindBurn     ChangeKind = "Burn"
	KindLoss     ChangeKind = "Loss"
	KindPayout   ChangeKind = "Payout"
	KindUnknown  ChangeKind = "Unknown"
)

type BalanceChange struct {
	Account     string
	Currency    string
	Issuer      string
	Delta       decimal.Decimal
	InitBalance decimal.Decimal
	Kind        ChangeKind
}

var (
	eps           = decimal.NewFromFloat(1e-100) // очень маленький порог для работы с числами до 100 знаков после запятой
	dustThreshold = decimal.NewFromFloat(1e-100) // всё меньше — считаем нулём (очень маленький порог)
)

func normalizeAmount(val decimal.Decimal) decimal.Decimal {
	abs := val.Abs()

	// Убираем только очень маленькую "пыль", но не ограничиваем большие числа
	if abs.LessThan(dustThreshold) {
		return decimal.Zero
	}

	// Не ограничиваем большие числа - они будут отфильтрованы позже при построении связок
	return val
}

type ActionKind string

const (
	ActTransfer ActionKind = "Transfer"
	ActSwap     ActionKind = "Swap"
	ActDexOffer ActionKind = "DexOffer"
	ActFee      ActionKind = "Fee"
	ActBurn     ActionKind = "Burn"
	ActPayout   ActionKind = "Payout"
	ActLoss     ActionKind = "Loss"
)

type Side struct {
	Account     string
	Currency    string
	Issuer      string
	Amount      decimal.Decimal
	InitBalance decimal.Decimal
}

type Action struct {
	Kind  ActionKind
	Sides []Side
	Note  string
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

func fixIssuerForXRP(currency, issuer string) string {
	if currency == "XRP" && issuer == "" {
		return "XRP"
	}
	return issuer
}

func generateVersion() uint64 {
	return uint64(time.Now().UnixNano())
}

// RunConsumer and RunBulkConsumer have been removed - Kafka is no longer used

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

				// Начальный баланс в XRP (до изменения)
				initBalance := balPrev.Div(decimal.NewFromInt(1_000_000))

				result = append(result, BalanceChange{
					Account:     account,
					Currency:    "XRP",
					Issuer:      "XRP",
					Delta:       delta,
					InitBalance: initBalance,
					Kind:        kind,
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

				// Определяем реального issuer токена из этой конкретной RippleState: тот, у кого value = 0
				// Если оба = 0, то смотрим на знак баланса
				realIssuer := determineRealIssuerWithBalance(highLimit, lowLimit, highIssuer, lowIssuer, balPrev, balFinal)

				// Если не смогли определить через RippleState, пробуем через поля транзакции
				if realIssuer == "" {
					realIssuer = detectTokenIssuer(base, currency)
				}

				// Определяем burn операцию: если токены отправляются эмитенту
				isBurn := false
				if txDestination != "" && txDestination == realIssuer && delta.IsPositive() {
					isBurn = true
				}

				// Определяем выдачу: если эмитент отправляет токен пользователю
				isPayout := false
				if txAccount == realIssuer && txDestination != realIssuer {
					// Для High: delta отрицательное = получатель получает токены (balance уменьшается в отрицательную сторону)
					// Для Low: delta положительное = получатель получает токены (balance увеличивается)
					if (highIssuer == txDestination && delta.IsNegative()) ||
						(lowIssuer == txDestination && delta.IsPositive()) {
						isPayout = true
					}
				}

				// сторона High (баланс ведётся от лица HighIssuer)
				// Не показываем изменения самого эмитента (у которого value = 0)
				if highIssuer != "" && highIssuer != realIssuer && !ammAccounts[highIssuer] && !(isSelfSwap && highIssuer != txAccount) {
					kind := KindUnknown
					if isBurn && highIssuer == txAccount {
						kind = KindBurn
					}
					if isPayout && highIssuer == txDestination {
						kind = KindPayout
					}
					// Начальный баланс для HighSide: -balPrev (баланс инвертирован)
					initBalanceHigh := balPrev.Neg()
					result = append(result, BalanceChange{
						Account:     highIssuer,
						Currency:    currency,
						Issuer:      realIssuer,
						Delta:       delta.Neg(),
						InitBalance: initBalanceHigh,
						Kind:        kind,
					})
				}
				// сторона Low
				// Не показываем изменения самого эмитента (у которого value = 0)
				if lowIssuer != "" && lowIssuer != realIssuer && !ammAccounts[lowIssuer] && !(isSelfSwap && lowIssuer != txAccount) {
					kind := KindUnknown
					if isBurn && lowIssuer == txAccount {
						kind = KindBurn
					}
					if isPayout && lowIssuer == txDestination {
						kind = KindPayout
					}
					// Начальный баланс для LowSide: balPrev
					initBalanceLow := balPrev
					result = append(result, BalanceChange{
						Account:     lowIssuer,
						Currency:    currency,
						Issuer:      realIssuer,
						Delta:       delta,
						InitBalance: initBalanceLow,
						Kind:        kind,
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
				// Начальный баланс = 0 (это новый аккаунт)
				result = append(result, BalanceChange{
					Account:     account,
					Currency:    "XRP",
					Issuer:      "XRP",
					Delta:       delta,
					InitBalance: decimal.Zero,
					Kind:        KindUnknown,
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

				// Определяем реального issuer токена из этой конкретной RippleState: тот, у кого value = 0
				// Если оба = 0, то смотрим на знак баланса
				realIssuer := determineRealIssuerWithBalance(highLimit, lowLimit, highIssuer, lowIssuer, decimal.Zero, balNew)

				// Если не смогли определить через RippleState, пробуем через поля транзакции
				if realIssuer == "" {
					realIssuer = detectTokenIssuer(base, currency)
				}

				// сторона High
				// Не показываем изменения самого эмитента (у которого value = 0)
				if highIssuer != "" && highIssuer != realIssuer && !ammAccounts[highIssuer] && !(isSelfSwap && highIssuer != txAccount) {
					// Для новой trustline начальный баланс = 0
					result = append(result, BalanceChange{
						Account:     highIssuer,
						Currency:    currency,
						Issuer:      realIssuer,
						Delta:       balNew.Neg(),
						InitBalance: decimal.Zero,
						Kind:        KindUnknown,
					})
				}
				// сторона Low
				// Не показываем изменения самого эмитента (у которого value = 0)
				if lowIssuer != "" && lowIssuer != realIssuer && !ammAccounts[lowIssuer] && !(isSelfSwap && lowIssuer != txAccount) {
					// Для новой trustline начальный баланс = 0
					result = append(result, BalanceChange{
						Account:     lowIssuer,
						Currency:    currency,
						Issuer:      realIssuer,
						Delta:       balNew,
						InitBalance: decimal.Zero,
						Kind:        KindUnknown,
					})
				}
			}
		}
	}

	return result
}

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

func determineRealIssuerWithBalance(highLimit, lowLimit map[string]interface{}, highIssuer, lowIssuer string, prevBalance decimal.Decimal, finalBalance decimal.Decimal) string {
	highValue, highOk := highLimit["value"].(string)
	lowValue, lowOk := lowLimit["value"].(string)

	highIsZero := highOk && highValue == "0"
	lowIsZero := lowOk && lowValue == "0"

	if highIsZero && !lowIsZero {
		return highIssuer
	}

	if lowIsZero && !highIsZero {
		return lowIssuer
	}

	if prevBalance.IsNegative() || finalBalance.IsNegative() {
		return lowIssuer
	}

	return highIssuer
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
	return val, true
}

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
		// Проверяем, что обе дельты в допустимом диапазоне
		if isWithinRange(changes[ip].Delta) && isWithinRange(changes[in].Delta) {
			acts = append(acts, Action{
				Kind: actionKind,
				Sides: []Side{
					{
						Account:     account,
						Currency:    changes[in].Currency,
						Issuer:      changes[in].Issuer,
						Amount:      changes[in].Delta, // минус
						InitBalance: changes[in].InitBalance,
					},
					{
						Account:     account,
						Currency:    changes[ip].Currency,
						Issuer:      changes[ip].Issuer,
						Amount:      changes[ip].Delta, // плюс
						InitBalance: changes[ip].InitBalance,
					},
				},
				Note: fmt.Sprintf("%s pair for %s", actionKind, account),
			})
			// проставим Kind у изменений
			switch actionKind {
			case ActSwap:
				changes[ip].Kind = KindSwap
				changes[in].Kind = KindSwap
			case ActDexOffer:
				changes[ip].Kind = KindDexOffer
				changes[in].Kind = KindDexOffer
			}
			// Помечаем как использованные только если связка прошла проверку диапазона
			used[ip] = true
			used[in] = true
		}
		// Если связка не прошла проверку диапазона, не помечаем как used,
		// чтобы эти изменения могли быть обработаны как Loss в конце
	}

	return
}

func BuildActionGroups(tx map[string]interface{}, changes []BalanceChange) []Action {
	var actions []Action
	used := make([]bool, len(changes))

	txAccount, _ := tx["Account"].(string)
	txDestination, _ := tx["Destination"].(string)

	// 1) Fee (явное)
	for i := range changes {
		if changes[i].Kind == KindFee && !used[i] {
			// Проверяем, что дельта в допустимом диапазоне
			if isWithinRange(changes[i].Delta) {
				actions = append(actions, Action{
					Kind: ActFee,
					Sides: []Side{{
						Account:     changes[i].Account,
						Currency:    "XRP",
						Issuer:      "XRP",
						Amount:      changes[i].Delta, // отрицательное
						InitBalance: changes[i].InitBalance,
					}},
					Note: "Fee",
				})
			}
			used[i] = true
		}
	}

	// 2) Burn (явное)
	for i := range changes {
		if changes[i].Kind == KindBurn && !used[i] {
			// Проверяем, что дельта в допустимом диапазоне
			if isWithinRange(changes[i].Delta) {
				actions = append(actions, Action{
					Kind: ActBurn,
					Sides: []Side{{
						Account:     changes[i].Account,
						Currency:    changes[i].Currency,
						Issuer:      changes[i].Issuer,
						Amount:      changes[i].Delta, // отрицательное (токены сжигаются)
						InitBalance: changes[i].InitBalance,
					}},
					Note: "Burn",
				})
			}
			used[i] = true
		}
	}

	// 3) Payout (явное)
	for i := range changes {
		if changes[i].Kind == KindPayout && !used[i] {
			// Проверяем, что дельта в допустимом диапазоне
			if isWithinRange(changes[i].Delta) {
				actions = append(actions, Action{
					Kind: ActPayout,
					Sides: []Side{{
						Account:     changes[i].Account,
						Currency:    changes[i].Currency,
						Issuer:      changes[i].Issuer,
						Amount:      changes[i].Delta,
						InitBalance: changes[i].InitBalance,
					}},
					Note: "Payout",
				})
			}
			used[i] = true
		}
	}

	// 4) Loss (явное)
	for i := range changes {
		if changes[i].Kind == KindLoss && !used[i] {
			// Проверяем, что дельта в допустимом диапазоне
			if isWithinRange(changes[i].Delta) {
				actions = append(actions, Action{
					Kind: ActLoss,
					Sides: []Side{{
						Account:     changes[i].Account,
						Currency:    "XRP",
						Issuer:      "XRP",
						Amount:      changes[i].Delta,
						InitBalance: changes[i].InitBalance,
					}},
					Note: "Loss",
				})
			}
			used[i] = true
		}
	}

	// 5) Свап у отправителя (если есть -XRP и +IOU/XRP)
	if txAccount != "" {
		acts := pairAccountActions(txAccount, changes, used, ActSwap)
		actions = append(actions, acts...)
	}

	// 6) DexOffer у остальных (каждый аккаунт, кроме отправителя и получателя)
	byAcct := collectByAccount(changes, used)
	for acct := range byAcct {
		if acct == txAccount || acct == txDestination {
			continue
		}
		acts := pairAccountActions(acct, changes, used, ActDexOffer)
		actions = append(actions, acts...)
	}

	// 7) Transfer: связать txAccount(оставшийся минус) -> txDestination(плюс delivered)
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
		// Проверяем, что обе дельты в допустимом диапазоне
		if isWithinRange(changes[senderNegIdx].Delta) && isWithinRange(changes[destPosIdx].Delta) {
			actions = append(actions, Action{
				Kind: ActTransfer,
				Sides: []Side{
					{
						Account:     changes[senderNegIdx].Account,
						Currency:    changes[senderNegIdx].Currency,
						Issuer:      changes[senderNegIdx].Issuer,
						Amount:      changes[senderNegIdx].Delta, // -
						InitBalance: changes[senderNegIdx].InitBalance,
					},
					{
						Account:     changes[destPosIdx].Account,
						Currency:    changes[destPosIdx].Currency,
						Issuer:      changes[destPosIdx].Issuer,
						Amount:      changes[destPosIdx].Delta, // +
						InitBalance: changes[destPosIdx].InitBalance,
					},
				},
				Note: "Transfer (sender->destination, possibly cross-currency)",
			})
			// пометим Kind у изменений
			changes[senderNegIdx].Kind = KindTransfer
			changes[destPosIdx].Kind = KindTransfer
			// Помечаем как использованные только если Transfer прошел проверку диапазона
			used[senderNegIdx] = true
			used[destPosIdx] = true
		}
		// Если Transfer не прошел проверку диапазона, не помечаем как used,
		// чтобы эти изменения могли быть обработаны как Loss в конце
	}

	// 8) Остатки — как Loss или Payout (редкие случаи, например, частичные/мульти-пары)
	for i := range changes {
		if used[i] {
			continue
		}
		// Проверяем, что дельта в допустимом диапазоне
		if isWithinRange(changes[i].Delta) {
			actionKind := ActLoss
			note := "Loss (unpaired residue)"
			if sign(changes[i].Delta) > 0 {
				actionKind = ActPayout
				note = "Payout (unpaired residue)"
			}
			actions = append(actions, Action{
				Kind: actionKind,
				Sides: []Side{{
					Account:     changes[i].Account,
					Currency:    changes[i].Currency,
					Issuer:      changes[i].Issuer,
					Amount:      changes[i].Delta,
					InitBalance: changes[i].InitBalance,
				}},
				Note: note,
			})
		}
	}

	return actions
}

func isWithinRange(val decimal.Decimal) bool {
	if val.IsZero() {
		return true
	}

	abs := val.Abs()

	// Получаем строковое представление с максимальной точностью (до 100 знаков после запятой)
	// StringFixed(100) даст нам число без научной нотации
	str := abs.StringFixed(100)

	// Разбираем строку на целую и дробную части
	parts := strings.Split(str, ".")
	var intPart, fracPart string

	if len(parts) == 1 {
		// Нет дробной части
		intPart = parts[0]
		fracPart = ""
	} else {
		intPart = parts[0]
		fracPart = parts[1]
	}

	// Убираем ведущие нули из целой части для подсчета значащих цифр
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}

	// Убираем trailing zeros из дробной части
	fracPart = strings.TrimRight(fracPart, "0")

	// Убираем leading zeros из дробной части для подсчета только значащих цифр
	// Ведущие нули в дробной части - это не значащие цифры, а просто позиция запятой
	fracPartSignificant := strings.TrimLeft(fracPart, "0")
	if fracPartSignificant == "" {
		fracPartSignificant = "0"
	}

	// Проверяем: не более 38 знаков до запятой
	if len(intPart) > 38 {
		return false
	}

	// Проверяем: не более 18 значащих цифр после запятой
	if len(fracPartSignificant) > 18 {
		return false
	}

	return true
}

// ProcessTransaction processes a transaction map and writes money flows directly to ClickHouse
// Returns the number of money flow rows written and any error
// This function can be called directly without Kafka
func ProcessTransaction(tx map[string]interface{}) (int, error) {
	var hash string
	if h, ok := tx["hash"].(string); ok {
		hash = h
	}

	// Check transaction type
	if tt, ok := tx["TransactionType"].(string); ok {
		if tt != "Payment" {
			// Only log if detailed logging is enabled (will check ledger_index later)
			return 0, nil // Skip non-Payment transactions
		}
	} else {
		// Only log if detailed logging is enabled (will check ledger_index later)
		return 0, nil // Skip transactions without type
	}

	modified, err := indexer.ModifyTransaction(tx)
	if err != nil {
		logger.Log.Error().Err(err).Msg("Error fixing transaction object")
		return 0, err
	}

	var base map[string]interface{} = modified
	if h, ok := base["hash"].(string); ok {
		hash = h
	}

	// Get ledger_index - it can be float64, int, or uint32
	var ledgerIndex float64
	if li, ok := base["ledger_index"].(float64); ok {
		ledgerIndex = li
	} else if li, ok := base["ledger_index"].(int); ok {
		ledgerIndex = float64(li)
	} else if li, ok := base["ledger_index"].(uint32); ok {
		ledgerIndex = float64(li)
	} else if li, ok := base["ledger_index"].(int64); ok {
		ledgerIndex = float64(li)
	} else {
		logger.Log.Error().
			Str("tx_hash", hash).
			Interface("ledger_index_type", base["ledger_index"]).
			Msg("Failed to extract ledger_index from transaction")
		return 0, fmt.Errorf("failed to extract ledger_index from transaction")
	}

	closeTime, _ := base["date"].(float64)
	inLedgerIndex := float64(0)

	// Check transaction result and get TransactionIndex from meta
	// Transactions in a ledger should have meta with TransactionResult
	// If meta is missing or TransactionResult is not tesSUCCESS, skip the transaction
	hasMeta := false
	if meta, ok := base["meta"].(map[string]interface{}); ok {
		hasMeta = true
		if r, ok := meta["TransactionResult"].(string); ok {
			if r != "tesSUCCESS" {
				// Only log if detailed logging is enabled for this ledger
				if config.ShouldLogDetailed(uint32(ledgerIndex)) {
					logger.Log.Debug().
						Str("tx_hash", hash).
						Uint32("ledger_index", uint32(ledgerIndex)).
						Str("transaction_result", r).
						Msg("Skipping failed transaction")
				}
				return 0, nil // Skip failed transactions
			}
		} else {
			// TransactionResult is missing in meta - this is unusual but we'll skip it to be safe
			if config.ShouldLogDetailed(uint32(ledgerIndex)) {
				logger.Log.Debug().
					Str("tx_hash", hash).
					Uint32("ledger_index", uint32(ledgerIndex)).
					Msg("Skipping transaction without TransactionResult in meta")
			}
			return 0, nil // Skip transactions without TransactionResult
		}
		if ti, ok := meta["TransactionIndex"].(float64); ok {
			inLedgerIndex = ti
		}
	} else {
		// Meta is missing - this is unusual for transactions in a ledger
		// We'll skip it to be safe, as we can't verify transaction success
		if config.ShouldLogDetailed(uint32(ledgerIndex)) {
			logger.Log.Debug().
				Str("tx_hash", hash).
				Uint32("ledger_index", uint32(ledgerIndex)).
				Msg("Skipping transaction without meta")
		}
		return 0, nil // Skip transactions without meta
	}

	// Only log detailed info if enabled for this ledger
	shouldLog := config.ShouldLogDetailed(uint32(ledgerIndex))
	if shouldLog {
		logger.Log.Debug().
			Str("tx_hash", hash).
			Uint32("ledger_index", uint32(ledgerIndex)).
			Uint32("in_ledger_index", uint32(inLedgerIndex)).
			Bool("has_meta", hasMeta).
			Msg("Processing Payment transaction")
	}
	feeDrops := uint64(0)
	switch v := base["Fee"].(type) {
	case float64:
		feeDrops = uint64(v)
	case int64:
		feeDrops = uint64(v)
	case int:
		feeDrops = uint64(v)
	case string:
		if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
			feeDrops = parsed
		}
	case json.Number:
		if parsed, err := v.Int64(); err == nil {
			feeDrops = uint64(parsed)
		}
	}

	const rippleToUnix int64 = 946684800
	closeTimeUnix := int64(closeTime) + rippleToUnix

	changes := ExtractBalanceChanges(base)
	actions := BuildActionGroups(base, changes)

	// Only log detailed info if enabled for this ledger
	if shouldLog {
		logger.Log.Debug().
			Str("tx_hash", hash).
			Uint32("ledger_index", uint32(ledgerIndex)).
			Int("balance_changes_count", len(changes)).
			Int("actions_count", len(actions)).
			Msg("Extracted balance changes and built action groups")
	}

	rowsWritten := 0
	for _, action := range actions {
		if len(action.Sides) == 0 {
			continue
		}

		var fromSide, toSide Side
		var kind string

		switch action.Kind {
		case ActFee:
			fromSide = action.Sides[0]
			toSide = Side{
				Account:     "",
				Currency:    "XRP",
				Issuer:      "XRP",
				Amount:      decimal.Zero,
				InitBalance: decimal.Zero,
			}
			kind = "fee"
		case ActBurn:
			fromSide = action.Sides[0]
			toSide = Side{
				Account:     "",
				Currency:    fromSide.Currency,
				Issuer:      fromSide.Issuer,
				Amount:      decimal.Zero,
				InitBalance: decimal.Zero,
			}
			kind = "burn"
		case ActPayout:
			toSide = action.Sides[0]
			fromSide = Side{
				Account:     "",
				Currency:    toSide.Currency,
				Issuer:      toSide.Issuer,
				Amount:      decimal.Zero,
				InitBalance: decimal.Zero,
			}
			kind = "payout"
		case ActLoss:
			fromSide = action.Sides[0]
			toSide = Side{
				Account:     "",
				Currency:    fromSide.Currency,
				Issuer:      fromSide.Issuer,
				Amount:      decimal.Zero,
				InitBalance: decimal.Zero,
			}
			kind = "loss"
		case ActSwap, ActDexOffer:
			if len(action.Sides) >= 2 {
				fromSide = action.Sides[0]
				toSide = action.Sides[1]
				if action.Kind == ActSwap {
					kind = "swap"
				} else {
					kind = "dexOffer"
				}
			} else {
				continue
			}
		case ActTransfer:
			if len(action.Sides) >= 2 {
				fromSide = action.Sides[0]
				toSide = action.Sides[1]
				kind = "transfer"
			} else {
				continue
			}
		default:
			continue
		}

		var rate decimal.Decimal = decimal.Zero
		if (action.Kind == ActSwap || action.Kind == ActDexOffer) && !toSide.Amount.IsZero() {
			rate = fromSide.Amount.Abs().Div(toSide.Amount.Abs())
		} else {
			rate = decimal.NewFromInt(1)
		}

		if fromSide.Account == BLACKLIST_ACCOUNT_ROGUE {
			continue
		}
		if toSide.Account == BLACKLIST_ACCOUNT_ROGUE {
			continue
		}

		row := models.CHMoneyFlowRow{
			TxHash:            hash,
			LedgerIndex:       uint32(ledgerIndex),
			InLedgerIndex:     uint32(inLedgerIndex),
			CloseTimeUnix:     closeTimeUnix,
			FeeDrops:          feeDrops,
			FromAddress:       fromSide.Account,
			ToAddress:         toSide.Account,
			FromCurrency:      fromSide.Currency,
			FromIssuerAddress: fixIssuerForXRP(fromSide.Currency, fromSide.Issuer),
			ToCurrency:        toSide.Currency,
			ToIssuerAddress:   fixIssuerForXRP(toSide.Currency, toSide.Issuer),
			FromAmount:        fromSide.Amount.String(),
			ToAmount:          toSide.Amount.String(),
			InitFromAmount:    fromSide.InitBalance.String(),
			InitToAmount:      toSide.InitBalance.String(),
			Quote:             rate.String(),
			Kind:              kind,
			Version:           generateVersion(),
		}

		// Write directly to ClickHouse
		if err := connections.WriteMoneyFlowRow(
			row.TxHash,
			row.LedgerIndex,
			row.InLedgerIndex,
			row.CloseTimeUnix,
			row.FeeDrops,
			row.FromAddress,
			row.ToAddress,
			row.FromCurrency,
			row.FromIssuerAddress,
			row.ToCurrency,
			row.ToIssuerAddress,
			row.FromAmount,
			row.ToAmount,
			row.InitFromAmount,
			row.InitToAmount,
			row.Quote,
			row.Kind,
			row.Version,
		); err != nil {
			logger.Log.Error().
				Err(err).
				Str("tx_hash", hash).
				Uint32("ledger_index", row.LedgerIndex).
				Str("kind", row.Kind).
				Msg("Failed to write money flow row to ClickHouse")
			return 0, err
		}
		rowsWritten++
	}

	// Only log detailed info if enabled for this ledger
	if shouldLog {
		if rowsWritten > 0 {
			logger.Log.Debug().
				Str("tx_hash", hash).
				Uint32("ledger_index", uint32(ledgerIndex)).
				Int("rows_written", rowsWritten).
				Msg("Successfully wrote money flow rows to ClickHouse batch")
		} else {
			logger.Log.Debug().
				Str("tx_hash", hash).
				Uint32("ledger_index", uint32(ledgerIndex)).
				Int("actions_count", len(actions)).
				Msg("No money flow rows written (all actions were skipped)")
		}
	}

	return rowsWritten, nil
}

func RunConsumers() {
	consumerWgMutex.Lock()
	consumerActive = true
	consumerWgMutex.Unlock()

	// Kafka consumers are no longer used - data is written directly to ClickHouse
	// This function is kept for compatibility but does nothing
}

// WaitForConsumersToFinish waits for all consumer messages to be processed
func WaitForConsumersToFinish() {
	consumerWgMutex.Lock()
	active := consumerActive
	consumerWgMutex.Unlock()

	if !active {
		return
	}

	logger.Log.Info().Msg("Waiting for all consumer messages to be processed...")
	consumerWg.Wait()
	logger.Log.Info().Msg("All consumer messages processed")
}
