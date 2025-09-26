package consumers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/xrpscan/platform/config"
	"github.com/xrpscan/platform/connections"
	"github.com/xrpscan/platform/indexer"
	"github.com/xrpscan/platform/logger"
	"github.com/xrpscan/platform/models"
)

var emittedAssets sync.Map

func idTx(hash string) string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("tx:"+hash)).String() }
func idAccount(addr string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("acct:"+addr)).String()
}
func idAssetXRP() string { return uuid.NewSHA1(uuid.NameSpaceURL, []byte("asset:XRP::")).String() }
func idAssetIOU(currency, issuer string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("asset:IOU:"+currency+":"+issuer)).String()
}

// Human-readable symbol resolution for IOU currencies
func decodeHexCurrency(cur string) (string, bool) {
	if len(cur) != 40 { // 20 bytes hex
		return "", false
	}
	b, err := hex.DecodeString(cur)
	if err != nil {
		return "", false
	}
	// trim trailing zero bytes
	end := len(b)
	for end > 0 && b[end-1] == 0x00 {
		end--
	}
	b = b[:end]
	if len(b) == 0 {
		return "", false
	}
	// ensure printable ASCII
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

func symbolFromCurrencyMap(m map[string]interface{}) string {
	if v, ok := m["_currency"].(string); ok && v != "" {
		return v
	}
	cur, _ := m["currency"].(string)
	if s, ok := decodeHexCurrency(cur); ok {
		return s
	}
	return cur
}

func normCurrency(c string) string { return strings.ToUpper(c) }

// Serial consumer (based on callback function) for low volume message streams
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

// Bulk message consumer (based on channel) for high volume message streams
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

// Run all consumers
func RunConsumers() {
	// Only run the transaction transformer; drop other noisy consumers
	go RunBulkConsumer(connections.KafkaReaderTransaction, func(ch <-chan kafka.Message) {
		ctx := context.Background()
		for {
			m := <-ch
			var tx map[string]interface{}
			if err := json.Unmarshal(m.Value, &tx); err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Unmarshal error")
				continue
			}
			// Filter by allowed transaction types at code level
			if tt, ok := tx["TransactionType"].(string); ok {
				if tt != "Payment" {
					// skip unwanted transaction types
					continue
				}
			} else {
				// unknown or missing type; skip
				continue
			}
			modified, err := indexer.ModifyTransaction(tx)
			if err != nil {
				logger.Log.Error().Err(err).Msg("Error fixing transaction object")
				continue
			}
			b, err := json.Marshal(modified)
			if err != nil {
				logger.Log.Error().Err(err).Msg("Transaction json.Marshal error")
				continue
			}

			// Emit CH rows: transactions, assets, money_flows
			var base map[string]interface{} = modified
			hash, _ := base["hash"].(string)
			ledgerIndex, _ := base["ledger_index"].(float64)
			closeTime, _ := base["date"].(float64)
			account, _ := base["Account"].(string)
			destination, _ := base["Destination"].(string)
			result := ""
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if r, ok := meta["TransactionResult"].(string); ok {
					result = r
				}
			}
			feeDrops := uint64(0)
			if fee, ok := base["Fee"].(float64); ok {
				feeDrops = uint64(fee)
			} else if feeStr, ok := base["Fee"].(string); ok {
				if v, err := strconv.ParseUint(feeStr, 10, 64); err == nil {
					feeDrops = v
				}
			}

			// transactions row (final JSON for CH MV)
			txId := idTx(hash)
			accountId := idAccount(account)
			destId := idAccount(destination)
			const rippleToUnix int64 = 946684800
			closeTimeUnix := int64(closeTime) + rippleToUnix
			txRow := models.CHTransactionRow{
				TxID:          txId,
				Hash:          hash,
				LedgerIndex:   uint32(ledgerIndex),
				CloseTimeUnix: closeTimeUnix,
				TxType:        "Payment",
				AccountID:     accountId,
				DestinationID: destId,
				Result:        result,
				FeeDrops:      feeDrops,
				RawJSON:       string(b),
			}
			if row, err := json.Marshal(txRow); err == nil {
				_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHTransactions(), Key: []byte(hash), Value: row})
			}

			// accounts rows: emit deduped final JSON rows for CH accounts
			if account != "" {
				aid := idAccount(account)
				if _, loaded := emittedAssets.LoadOrStore("acc:"+account, true); !loaded {
					ar := models.CHAccountRow{AccountID: aid, Address: account}
					if row, err := json.Marshal(ar); err == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(account), Value: row})
					}
				}
			}
			if destination != "" {
				did := idAccount(destination)
				if _, loaded := emittedAssets.LoadOrStore("acc:"+destination, true); !loaded {
					dr := models.CHAccountRow{AccountID: did, Address: destination}
					if row, err := json.Marshal(dr); err == nil {
						_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(destination), Value: row})
					}
				}
			}

			// assets rows from tx fields (preferred canonical sources)
			issuersByCurrency := make(map[string]string)
			// 1) Amount
			if amt, ok := base["Amount"].(map[string]interface{}); ok {
				cur, _ := amt["currency"].(string)
				cur = normCurrency(cur)
				iss, _ := amt["issuer"].(string)
				if cur != "" && iss != "" {
					issuersByCurrency[cur] = iss
					assetKey := "IOU:" + cur + ":" + iss
					if _, loaded := emittedAssets.LoadOrStore(assetKey, true); !loaded {
						sym := symbolFromCurrencyMap(amt)
						issuerUUID := idAccount(iss)
						assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym}
						if row, err := json.Marshal(assetRow); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
						}
						if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
							ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
							if row2, err2 := json.Marshal(ar); err2 == nil {
								_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
							}
						}
					}
				}
			}
			// 2) SendMax
			if sm, ok := base["SendMax"].(map[string]interface{}); ok {
				cur, _ := sm["currency"].(string)
				cur = normCurrency(cur)
				iss, _ := sm["issuer"].(string)
				if cur != "" && iss != "" {
					if _, ok := issuersByCurrency[cur]; !ok {
						issuersByCurrency[cur] = iss
					}
					assetKey := "IOU:" + cur + ":" + iss
					if _, loaded := emittedAssets.LoadOrStore(assetKey, true); !loaded {
						sym := symbolFromCurrencyMap(sm)
						issuerUUID := idAccount(iss)
						assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym}
						if row, err := json.Marshal(assetRow); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
						}
						if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
							ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
							if row2, err2 := json.Marshal(ar); err2 == nil {
								_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
							}
						}
					}
				}
			}
			// 3) meta.delivered_amount
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if da, ok := meta["delivered_amount"].(map[string]interface{}); ok {
					cur, _ := da["currency"].(string)
					cur = normCurrency(cur)
					iss, _ := da["issuer"].(string)
					if cur != "" && iss != "" {
						issuersByCurrency[cur] = iss
						assetKey := "IOU:" + cur + ":" + iss
						if _, loaded := emittedAssets.LoadOrStore(assetKey, true); !loaded {
							sym := symbolFromCurrencyMap(da)
							issuerUUID := idAccount(iss)
							assetRow := models.CHAssetRow{AssetID: idAssetIOU(cur, iss), AssetType: "IOU", Currency: cur, IssuerID: issuerUUID, Symbol: sym}
							if row, err := json.Marshal(assetRow); err == nil {
								_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAssets(), Key: []byte(assetKey), Value: row})
							}
							if _, loaded2 := emittedAssets.LoadOrStore("acc:"+iss, true); !loaded2 {
								ar := models.CHAccountRow{AccountID: issuerUUID, Address: iss}
								if row2, err2 := json.Marshal(ar); err2 == nil {
									_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHAccounts(), Key: []byte(iss), Value: row2})
								}
							}
						}
					}
				}
			}

			// money_flow rows derived from AffectedNodes:
			// - transfer: AccountRoot Balance deltas (XRP)
			// - dexOffer/swap: Offer execution inferred from Balance deltas across two assets
			if meta, ok := base["meta"].(map[string]interface{}); ok {
				if nodes, ok := meta["AffectedNodes"].([]interface{}); ok {
					// First pass: collect balance deltas (XRP and IOU) per account+asset
					type assetKey struct{ currency, issuer string }
					balances := make(map[string]map[assetKey]decimal.Decimal)
					for _, n := range nodes {
						node, _ := n.(map[string]interface{})
						var fields map[string]interface{}
						var obj map[string]interface{}
						if created, ok := node["CreatedNode"].(map[string]interface{}); ok {
							obj = created
							fields, _ = created["NewFields"].(map[string]interface{})
						} else if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
							obj = modified
							fields, _ = modified["FinalFields"].(map[string]interface{})
						} else if deleted, ok := node["DeletedNode"].(map[string]interface{}); ok {
							obj = deleted
							fields, _ = deleted["FinalFields"].(map[string]interface{})
						}
						if fields == nil {
							continue
						}
						ledgerType, _ := obj["LedgerEntryType"].(string)
						switch ledgerType {
						case "AccountRoot":
							addr, _ := fields["Account"].(string)
							if addr == "" {
								continue
							}
							var prevBalance, finalBalance int64
							if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
								if pf, ok := modified["PreviousFields"].(map[string]interface{}); ok {
									if pb, ok := pf["Balance"].(string); ok {
										if v, err := strconv.ParseInt(pb, 10, 64); err == nil {
											prevBalance = v
										}
									}
								}
								if ff, ok := modified["FinalFields"].(map[string]interface{}); ok {
									if fb, ok := ff["Balance"].(string); ok {
										if v, err := strconv.ParseInt(fb, 10, 64); err == nil {
											finalBalance = v
										}
									}
								}
							}
							if prevBalance != 0 || finalBalance != 0 {
								delta := decimal.NewFromInt(finalBalance - prevBalance).Div(decimal.NewFromInt(int64(models.DROPS_IN_XRP)))
								if !delta.IsZero() {
									if _, ok := balances[addr]; !ok {
										balances[addr] = make(map[assetKey]decimal.Decimal)
									}
									k := assetKey{currency: "XRP", issuer: ""}
									balances[addr][k] = balances[addr][k].Add(delta)
								}
							}
						case "RippleState":
							// IOU trustline balance delta
							// Extract account, currency, issuer and delta from PreviousFields/FinalFields.Balance.value
							high, _ := fields["HighLimit"].(map[string]interface{})
							low, _ := fields["LowLimit"].(map[string]interface{})
							bal, _ := fields["Balance"].(map[string]interface{})
							currency, _ := bal["currency"].(string)
							currency = normCurrency(currency)
							issuerHigh, _ := high["issuer"].(string)
							issuerLow, _ := low["issuer"].(string)
							// Determine which side increased/decreased by comparing PreviousFields/FinalFields
							var prevV, finalV decimal.Decimal
							if modified, ok := node["ModifiedNode"].(map[string]interface{}); ok {
								if pf, ok := modified["PreviousFields"].(map[string]interface{}); ok {
									if pb, ok := pf["Balance"].(map[string]interface{}); ok {
										if vs, ok := pb["value"].(string); ok {
											prevV, _ = decimal.NewFromString(vs)
										}
									}
								}
								if ff, ok := modified["FinalFields"].(map[string]interface{}); ok {
									if fb, ok := ff["Balance"].(map[string]interface{}); ok {
										if vs, ok := fb["value"].(string); ok {
											finalV, _ = decimal.NewFromString(vs)
										}
									}
								}
							}
							if prevV.Equal(finalV) {
								continue
							}
							// RippleState.balance is from low->high perspective
							// delta > 0 => transfer from high to low (low receives)
							abs := finalV.Sub(prevV)
							recv := issuerLow
							send := issuerHigh
							if abs.IsNegative() {
								// delta < 0 => transfer low -> high
								abs = abs.Neg()
								recv = issuerHigh
								send = issuerLow
							}
							issuer := issuersByCurrency[currency]
							if _, ok := balances[recv]; !ok {
								balances[recv] = make(map[assetKey]decimal.Decimal)
							}
							if _, ok := balances[send]; !ok {
								balances[send] = make(map[assetKey]decimal.Decimal)
							}
							k := assetKey{currency: currency, issuer: issuer}
							balances[recv][k] = balances[recv][k].Add(abs) // receiver +abs
							balances[send][k] = balances[send][k].Sub(abs) // sender -abs

							// Do not emit assets from RippleState to avoid issuer ambiguity
						}
					}

					// Build edges by matching positive and negative deltas per asset
					epsilon := decimal.New(1, -12) // 1e-12 to drop dust
					type edge struct {
						from, to string
						asset    assetKey
						amount   decimal.Decimal
					}
					edges := make([]edge, 0)
					for ak := range func(m map[string]map[assetKey]decimal.Decimal) map[assetKey]struct{} {
						u := make(map[assetKey]struct{})
						for _, mm := range m {
							for k := range mm {
								u[k] = struct{}{}
							}
						}
						return u
					}(balances) {
						// collect sources and sinks for this asset
						type pair struct {
							addr string
							amt  decimal.Decimal
						}
						sources := make([]pair, 0)
						sinks := make([]pair, 0)
						for addr, mm := range balances {
							amt := mm[ak]
							if amt.IsZero() {
								continue
							}
							if amt.IsNegative() {
								sources = append(sources, pair{addr, amt.Neg()})
							} else {
								sinks = append(sinks, pair{addr, amt})
							}
						}
						// greedy match
						i, j := 0, 0
						for i < len(sources) && j < len(sinks) {
							s := sources[i]
							t := sinks[j]
							take := decimal.Min(s.amt, t.amt)
							if take.IsZero() || take.LessThan(epsilon) {
								break
							}
							edges = append(edges, edge{from: s.addr, to: t.addr, asset: ak, amount: take})
							s.amt = s.amt.Sub(take)
							t.amt = t.amt.Sub(take)
							if s.amt.IsZero() {
								i++
							} else {
								sources[i] = s
							}
							if t.amt.IsZero() {
								j++
							} else {
								sinks[j] = t
							}
						}
					}

					// Heuristics to classify kind per edge
					// Determine if initiator participates in multi-asset (swap)
					initiator := account
					seenAssetsForInitiator := 0
					uniq := make(map[assetKey]struct{})
					for _, e := range edges {
						if e.from == initiator || e.to == initiator {
							if _, ok := uniq[e.asset]; !ok {
								uniq[e.asset] = struct{}{}
								seenAssetsForInitiator++
							}
						}
					}

					totalXrp := decimal.Zero
					totalIou := decimal.Zero
					for _, e := range edges {
						if e.asset.currency == "XRP" {
							if e.amount.IsNegative() {
								totalXrp = totalXrp.Add(e.amount.Neg())
							} else {
								totalXrp = totalXrp.Add(e.amount)
							}
						} else {
							if e.amount.IsNegative() {
								totalIou = totalIou.Add(e.amount.Neg())
							} else {
								totalIou = totalIou.Add(e.amount)
							}
						}
					}
					rate := decimal.Zero
					if !totalIou.IsZero() {
						rate = totalXrp.Div(totalIou)
					}

					for _, e := range edges {
						// positive valuation in XRP units (magnitude), independent of direction
						quoteAbs := decimal.Zero
						if e.asset.currency == "XRP" {
							quoteAbs = e.amount
							if quoteAbs.IsNegative() {
								quoteAbs = quoteAbs.Neg()
							}
						} else if !rate.IsZero() {
							q := e.amount
							if q.IsNegative() {
								q = q.Neg()
							}
							quoteAbs = q.Mul(rate)
						}
						kind := "transfer"
						if (e.from == initiator || e.to == initiator) && seenAssetsForInitiator >= 2 {
							kind = "swap"
						} else if e.asset.currency != "XRP" {
							kind = "dexOffer"
						}
						// compute IDs
						accFrom := idAccount(e.from)
						accTo := idAccount(e.to)
						var assetId string
						if e.asset.currency == "XRP" {
							assetId = idAssetXRP()
						} else {
							assetId = idAssetIOU(normCurrency(e.asset.currency), e.asset.issuer)
						}
						// receiver (credit, +amount); ensure positive magnitude
						amtAbs := e.amount
						if amtAbs.IsNegative() {
							amtAbs = amtAbs.Neg()
						}
						if amtAbs.LessThan(epsilon) {
							continue
						}
						mfRecv := models.CHMoneyFlowRow{TxID: txId, FromID: accFrom, ToID: accTo, AssetID: assetId, Amount: amtAbs.String(), QuoteXRP: quoteAbs.String(), Kind: kind}
						if row, err := json.Marshal(mfRecv); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHMoneyFlows(), Key: []byte(hash), Value: row})
						}
						// sender (debit, -amount)
						mfSend := models.CHMoneyFlowRow{TxID: txId, FromID: accFrom, ToID: accTo, AssetID: assetId, Amount: amtAbs.Neg().String(), QuoteXRP: quoteAbs.String(), Kind: kind}
						if row, err := json.Marshal(mfSend); err == nil {
							_ = connections.KafkaWriter.WriteMessages(ctx, kafka.Message{Topic: config.TopicCHMoneyFlows(), Key: []byte(hash), Value: row})
						}
					}
				}
			}
		}
	})
}
