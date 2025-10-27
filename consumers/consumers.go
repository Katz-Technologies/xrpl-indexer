package consumers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

const BLACKLIST_ACCOUNT_ROGUE = "rogue5HnPRSszD9CWGSUz8UGHMVwSSKF6"

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
	eps           = decimal.NewFromFloat(1e-9)
	dustThreshold = decimal.NewFromFloat(1e-18)
	maxIOUValue   = decimal.NewFromFloat(1e38)
)

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

func decodeHexCurrency(cur string) (string, bool) {
	if len(cur) != 40 {
		return "", false
	}
	b, err := hex.DecodeString(cur)
	if err != nil {
		return "", false
	}
	end := len(b)
	for end > 0 && b[end-1] == 0x00 {
		end--
	}
	b = b[:end]
	if len(b) == 0 {
		return "", false
	}
	for _, c := range b {
		if c < 32 || c > 126 {
			return "", false
		}
	}
	s := string(b)
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	return s, true
}

func isHexCurrency(cur string) bool {
	if len(cur) != 40 {
		return false
	}
	for _, c := range cur {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func currencyToSymbol(cur string) string {
	if isHexCurrency(cur) {
		if symbol, ok := decodeHexCurrency(cur); ok {
			return symbol
		}
		return ""
	}
	return strings.ToUpper(cur)
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

func RunConsumer(conn *kafka.Reader, callback func(m kafka.Message)) {
	ctx := context.Background()
	for {
		m, err := conn.FetchMessage(ctx)
		if err != nil {
			break
		}
		callback(m)

		if err := conn.CommitMessages(ctx, m); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to commit kafka message")
		}
	}
}

func RunBulkConsumer(conn *kafka.Reader, callback func(<-chan kafka.Message)) {
	ctx := context.Background()
	ch := make(chan kafka.Message)
	go callback(ch)

	for {
		m, err := conn.FetchMessage(ctx)
		if err != nil {
			break
		}

		ch <- m

		if err := conn.CommitMessages(ctx, m); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to commit kafka message")
		}
	}
}

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

	isSelfSwap := txAccount != "" && txAccount == txDestination && !hasOffer

	for _, raw := range nodes {
		nodeMap, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}

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
				delta := normalizeAmount(balFinal.Sub(balPrev).Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP))))

				if delta.IsZero() {
					continue
				}

				kind := KindUnknown

				if account == txAccount && delta.IsNegative() && delta.Abs().Equal(txFee.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP))).Abs()) {
					kind = KindFee
				}

				initBalance := balPrev.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))

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

				realIssuer := determineRealIssuerWithBalance(highLimit, lowLimit, highIssuer, lowIssuer, balFinal)

				isBurn := false
				if txDestination != "" && txDestination == realIssuer && delta.IsPositive() {
					isBurn = true
				}

				isPayout := false
				if txAccount == realIssuer && txDestination != realIssuer {
					if (highIssuer == txDestination && delta.IsNegative()) ||
						(lowIssuer == txDestination && delta.IsPositive()) {
						isPayout = true
					}
				}

				if highIssuer != "" && highIssuer != realIssuer && !ammAccounts[highIssuer] && !(isSelfSwap && highIssuer != txAccount) {
					kind := KindUnknown
					if isBurn && highIssuer == txAccount {
						kind = KindBurn
					}
					if isPayout && highIssuer == txDestination {
						kind = KindPayout
					}
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
				if lowIssuer != "" && lowIssuer != realIssuer && !ammAccounts[lowIssuer] && !(isSelfSwap && lowIssuer != txAccount) {
					kind := KindUnknown
					if isBurn && lowIssuer == txAccount {
						kind = KindBurn
					}
					if isPayout && lowIssuer == txDestination {
						kind = KindPayout
					}
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
				delta := normalizeAmount(balNew.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP))))

				if delta.IsZero() {
					continue
				}

				result = append(result, BalanceChange{
					Account:     account,
					Currency:    "XRP",
					Issuer:      "XRP",
					Delta:       delta,
					InitBalance: decimal.Zero,
					Kind:        KindUnknown,
				})

			case "RippleState":
				balNew, ok1 := extractDecimal(newFields, "Balance", "value")
				if !ok1 {
					continue
				}

				deltaNew := normalizeAmount(balNew)
				if deltaNew.IsZero() {
					continue
				}

				highLimit, _ := newFields["HighLimit"].(map[string]interface{})
				lowLimit, _ := newFields["LowLimit"].(map[string]interface{})
				highIssuer, _ := highLimit["issuer"].(string)
				lowIssuer, _ := lowLimit["issuer"].(string)
				currency, _ := highLimit["currency"].(string)

				realIssuer := determineRealIssuerWithBalance(highLimit, lowLimit, highIssuer, lowIssuer, balNew)

				if highIssuer != "" && highIssuer != realIssuer && !ammAccounts[highIssuer] && !(isSelfSwap && highIssuer != txAccount) {
					result = append(result, BalanceChange{
						Account:     highIssuer,
						Currency:    currency,
						Issuer:      realIssuer,
						Delta:       deltaNew.Neg(),
						InitBalance: decimal.Zero,
						Kind:        KindUnknown,
					})
				}
				if lowIssuer != "" && lowIssuer != realIssuer && !ammAccounts[lowIssuer] && !(isSelfSwap && lowIssuer != txAccount) {
					result = append(result, BalanceChange{
						Account:     lowIssuer,
						Currency:    currency,
						Issuer:      realIssuer,
						Delta:       deltaNew,
						InitBalance: decimal.Zero,
						Kind:        KindUnknown,
					})
				}
			}
		}
	}

	return result
}

func determineRealIssuerWithBalance(highLimit, lowLimit map[string]interface{}, highIssuer, lowIssuer string, balance decimal.Decimal) string {
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

	if highIsZero && lowIsZero {
		if balance.IsNegative() {
			return lowIssuer
		} else {
			return highIssuer
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

	if meta, okm := tx["meta"].(map[string]interface{}); okm {
		if cur, iss, v, okk := tryObj(meta["delivered_amount"]); okk {
			return cur, iss, v, true, true
		}
		if cur, iss, v, okk := tryObj(meta["DeliveredAmount"]); okk {
			return cur, iss, v, true, true
		}
		if s, oks := meta["delivered_amount"].(string); oks && s != "" {
			if drops, err := decimal.NewFromString(s); err == nil {
				return "XRP", "XRP", drops.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP))), false, true
			}
		}
		if s, oks := meta["DeliveredAmount"].(string); oks && s != "" {
			if drops, err := decimal.NewFromString(s); err == nil {
				return "XRP", "XRP", drops.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP))), false, true
			}
		}
	}

	switch a := tx["Amount"].(type) {
	case string:
		if a != "" {
			if drops, err := decimal.NewFromString(a); err == nil {
				return "XRP", "XRP", drops.Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP))), false, true
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
		used[ip] = true
		used[in] = true
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

func BuildActionGroups(tx map[string]interface{}, changes []BalanceChange) []Action {
	var actions []Action
	used := make([]bool, len(changes))

	txAccount, _ := tx["Account"].(string)
	txDestination, _ := tx["Destination"].(string)

	for i := range changes {
		if changes[i].Kind == KindFee && !used[i] {
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
			used[i] = true
		}
	}

	for i := range changes {
		if changes[i].Kind == KindBurn && !used[i] {
			actions = append(actions, Action{
				Kind: ActBurn,
				Sides: []Side{{
					Account:     changes[i].Account,
					Currency:    changes[i].Currency,
					Issuer:      changes[i].Issuer,
					Amount:      changes[i].Delta,
					InitBalance: changes[i].InitBalance,
				}},
				Note: "Burn",
			})
			used[i] = true
		}
	}

	for i := range changes {
		if changes[i].Kind == KindPayout && !used[i] {
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
			used[i] = true
		}
	}

	for i := range changes {
		if changes[i].Kind == KindLoss && !used[i] {
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
			used[i] = true
		}
	}

	if txAccount != "" {
		acts := pairAccountActions(txAccount, changes, used, ActSwap)
		actions = append(actions, acts...)
	}

	byAcct := collectByAccount(changes, used)
	for acct := range byAcct {
		if acct == txAccount || acct == txDestination {
			continue
		}
		acts := pairAccountActions(acct, changes, used, ActDexOffer)
		actions = append(actions, acts...)
	}

	delCur, delIss, delVal, _, hasDel := getDeliveredInfo(tx)

	var destPosIdx = -1
	if hasDel && txDestination != "" {
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
		used[senderNegIdx] = true
		used[destPosIdx] = true
		changes[senderNegIdx].Kind = KindTransfer
		changes[destPosIdx].Kind = KindTransfer
	}

	for i := range changes {
		if used[i] {
			continue
		}
		actions = append(actions, Action{
			Kind: ActLoss,
			Sides: []Side{{
				Account:     changes[i].Account,
				Currency:    changes[i].Currency,
				Issuer:      changes[i].Issuer,
				Amount:      changes[i].Delta,
				InitBalance: changes[i].InitBalance,
			}},
			Note: "Loss (unpaired residue)",
		})
	}

	return actions
}

func RunConsumers() {
	go RunBulkConsumer(connections.KafkaReaderTransaction, func(ch <-chan kafka.Message) {
		ctx := context.Background()
		for {
			m := <-ch
			var tx map[string]interface{}
			if err := json.Unmarshal(m.Value, &tx); err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Unmarshal error")
				continue
			}
			if tt, ok := tx["TransactionType"].(string); ok {
				if tt != "Payment" {
					continue
				}
			} else {
				continue
			}

			modified, err := indexer.ModifyTransaction(tx)
			if err != nil {
				logger.Log.Error().Err(err).Msg("Error fixing transaction object")
				continue
			}

			var base map[string]interface{} = modified
			hash, _ := base["hash"].(string)
			ledgerIndex, _ := base["ledger_index"].(float64)
			closeTime, _ := base["date"].(float64)
			inLedgerIndex := float64(0)
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if r, ok := meta["TransactionResult"].(string); ok {
					if r != "tesSUCCESS" {
						continue
					}
				}
				if ti, ok := meta["TransactionIndex"].(float64); ok {
					inLedgerIndex = ti
				}
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
					FromCurrency:      currencyToSymbol(fromSide.Currency),
					FromIssuerAddress: fixIssuerForXRP(currencyToSymbol(fromSide.Currency), fromSide.Issuer),
					ToCurrency:        currencyToSymbol(toSide.Currency),
					ToIssuerAddress:   fixIssuerForXRP(currencyToSymbol(toSide.Currency), toSide.Issuer),
					FromAmount:        fromSide.Amount.String(),
					ToAmount:          toSide.Amount.String(),
					InitFromAmount:    fromSide.InitBalance.String(),
					InitToAmount:      toSide.InitBalance.String(),
					Quote:             rate.String(),
					Kind:              kind,
					Version:           generateVersion(),
				}

				if rowData, err := json.Marshal(row); err == nil {
					_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHMoneyFlows(), Key: []byte(hash), Value: rowData})
				}
			}
		}
	})
}
